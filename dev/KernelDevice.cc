// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <vector>

#include "KernelDevice.h"
#include "common/utime.h"

static thread_local int cur_thread = 0;

KernelDevice::KernelDevice(BlueFSContext* c, aio_callback_t cb, void *cbpriv)
  : BlockDevice(c),
    size(0), block_size(0),
    aio(false), dio(false),
    thread_num(c->_conf->thread_per_dev),
    aio_callback(cb),
    aio_callback_priv(cbpriv)
{
    if (thread_num > MAX_DEV_THREAD) {
        thread_num = MAX_DEV_THREAD;
    }

    cur_thread = 0;
    fd_directs.resize(thread_num);
    fd_buffereds.resize(thread_num);
    aio_queues.resize(thread_num);
    aio_stops.resize(thread_num);
    for (int i = 0; i < thread_num; i++) {
        fd_directs[i] = -1;
        fd_buffereds[i] = -1;
        aio_queues[i] = aio_queue_t(cct->_conf->bdev_aio_max_queue_depth);
        aio_stops[i] = false;
    }
}

int KernelDevice::_lock()
{
    struct flock l;
    memset(&l, 0, sizeof(l));
    l.l_type = F_WRLCK;
    l.l_whence = SEEK_SET;
    int r = ::fcntl(fd_directs[0], F_SETLK, &l);
    if (r < 0)
        return -errno;
    return 0;
    }

int KernelDevice::open(const std::string& p)
{
    path = p;
    int r = 0;
    dout(1) << __func__ << " path " << path << dendl;

    for (int i = 0; i < thread_num; i++) {
        fd_directs[i] = ::open(path.c_str(), O_RDWR | O_DIRECT | O_CLOEXEC);
        if (fd_directs[i] < 0) {
            r = -errno;
            derr << __func__ << " open got: " << cpp_strerror(r) << dendl;
            return r;
        }
        fd_buffereds[i] = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
        if (fd_buffereds[i] < 0) {
            r = -errno;
            derr << __func__ << " open got: " << cpp_strerror(r) << dendl;
            goto out_direct;
        }
    }

    dio = true;
    aio = cct->_conf->bdev_aio;
    if (!aio) {
        assert(0 == "non-aio not supported");
    }

    // disable readahead as it will wreak havoc on our mix of
    // directio/aio and buffered io.
    r = posix_fadvise(fd_buffereds[0], 0, 0, POSIX_FADV_RANDOM);
    if (r) {
        r = -r;
        derr << __func__ << " open got: " << cpp_strerror(r) << dendl;
        goto out_fail;
    }

    r = _lock();
    if (r < 0) {
        derr << __func__ << " failed to lock " << path << ": " << cpp_strerror(r)
        << dendl;
        goto out_fail;
    }

    struct stat st;
    r = ::fstat(fd_directs[0], &st);
    if (r < 0) {
        r = -errno;
        derr << __func__ << " fstat got " << cpp_strerror(r) << dendl;
        goto out_fail;
    }

    // Operate as though the block size is 4 KB.  The backing file
    // blksize doesn't strictly matter except that some file systems may
    // require a read/modify/write if we write something smaller than
    // it.
    block_size = cct->_conf->bdev_block_size;
    if (block_size != (unsigned)st.st_blksize) {
        dout(1) << __func__ << " backing device/file reports st_blksize "
            << st.st_blksize << ", using bdev_block_size "
            << block_size << " anyway" << dendl;
    }

    if (S_ISBLK(st.st_mode)) {
        int64_t s;
        r = get_block_device_size(fd_directs[0], &s);
        if (r < 0) {
            goto out_fail;
        }
        size = s;
    } else {
        size = st.st_size;
    }

    r = _aio_start();
    if (r < 0) {
        goto out_fail;
    }

    // round size down to an even block
    size &= ~(block_size - 1);

    dout(1) << __func__
        << " size " << size
        << " (0x" << std::hex << size << std::dec << ", "
        << byte_u_t(size) << ")"
        << " block_size " << block_size
        << " (" << byte_u_t(block_size) << ")"
        << dendl;
    return 0;

out_fail:
    for (int i = 0; i < thread_num; i++) {
        ::close(fd_buffereds[i]);
        fd_buffereds[i] = -1;
    }
out_direct:
    for (int i = 0; i < thread_num; i++) {
        ::close(fd_directs[i]);
        fd_directs[i] = -1;
    }
    return r;
}

void KernelDevice::close()
{
    dout(1) << __func__ << dendl;
    _aio_stop();

    for (int i = 0; i < thread_num; i++) {
        assert(fd_directs[i] >= 0);
        ::close(fd_directs[i]);
        fd_directs[i] = -1;
    }

    for (int i = 0; i < thread_num; i++) {
        assert(fd_buffereds[i] >= 0);
        ::close(fd_buffereds[i]);
        fd_buffereds[i] = -1;
    }

    path.clear();
}

int KernelDevice::flush()
{
    // protect flush with a mutex.  note that we are not really protecting
    // data here.  instead, we're ensuring that if any flush() caller
    // sees that io_since_flush is true, they block any racing callers
    // until the flush is observed.  that allows racing threads to be
    // calling flush while still ensuring that *any* of them that got an
    // aio completion notification will not return before that aio is
    // stable on disk: whichever thread sees the flag first will block
    // followers until the aio is stable.
    std::lock_guard<std::mutex> l(flush_mutex);

    bool expect = true;
    if (!io_since_flush.compare_exchange_strong(expect, false)) {
        dout(10) << __func__ << " no-op (no ios since last flush), flag is "
            << (int)io_since_flush.load() << dendl;
        return 0;
    }

    dout(10) << __func__ << " start" << dendl;
    utime_t start = clock_now();
    int r = ::fdatasync(fd_directs[0]);
    utime_t end = clock_now();
    utime_t dur = end - start;
    if (r < 0) {
        r = -errno;
        derr << __func__ << " fdatasync got: " << cpp_strerror(r) << dendl;
        abort();
    }
    dout(5) << __func__ << " in " << dur << dendl;;
    return r;
}

int KernelDevice::_aio_start()
{
    if (aio) {
        dout(10) << __func__ << dendl;
        for (int i = 0; i < thread_num; i++) {
            int r = aio_queues[i].init();
            if (r < 0) {
                if (r == -EAGAIN) {
                    derr << __func__ << " io_setup(2) failed with EAGAIN; "
                         << "try increasing /proc/sys/fs/aio-max-nr" << dendl;
                } else {
                    derr << __func__ << " io_setup(2) failed: " << cpp_strerror(r) << dendl;
                }
                return r;
            }
            std::string name = "bluefs_aio" + std::to_string(i);
            aio_threads[i] = std::thread{ &KernelDevice::_aio_thread, this, i};
            pthread_setname_np(aio_threads[i].native_handle(), name.c_str());
        }
    }
    return 0;
}

void KernelDevice::_aio_stop()
{
    if (aio) {
        dout(10) << __func__ << dendl;
        for (int i = 0; i < thread_num; i++) {
            aio_stops[i] = true;
            aio_threads[i].join();
            aio_stops[i] = false;
            aio_queues[i].shutdown();
        }
    }
}

static bool is_expected_ioerr(const int r)
{
    return (r == -EOPNOTSUPP || r == -ETIMEDOUT || r == -ENOSPC ||
        r == -ENOLINK || r == -EREMOTEIO || r == -EBADE ||
        r == -ENODATA || r == -EILSEQ || r == -ENOMEM ||
        r == -EAGAIN || r == -EREMCHG || r == -EIO);
}

void KernelDevice::_aio_thread(int idx)
{
    dout(10) << __func__ << " start" << dendl;
    while (!aio_stops[idx]) {
        dout(40) << __func__ << " polling" << dendl;
        int max = cct->_conf->bdev_aio_reap_max;
        aio_t *aio_s[max];
        int r = aio_queues[idx].get_next_completed(cct->_conf->bdev_aio_poll_ms,
                                             aio_s, max);
        if (r < 0) {
            derr << __func__ << " got " << cpp_strerror(r) << dendl;
            assert(0 == "got unexpected error from io_getevents");
        }
        if (r > 0) {
            dout(30) << __func__ << " got " << r << " completed aios" << dendl;
            for (int i = 0; i < r; ++i) {
                IOContext *ioc = static_cast<IOContext*>(aio_s[i]->priv);

                // set flag indicating new ios have completed.  we do this *before*
                // any completion or notifications so that any user flush() that
                // follows the observed io completion will include this io.  Note
                // that an earlier, racing flush() could observe and clear this
                // flag, but that also ensures that the IO will be stable before the
                // later flush() occurs.
                io_since_flush.store(true);

                long ret = aio_s[i]->get_return_value();
                if (ret < 0) {
                    derr << __func__ << " got r=" << ret << " (" << cpp_strerror(ret) << ")"
                    << dendl;
                    if (ioc->allow_eio && is_expected_ioerr(ret)) {
                        derr << __func__ << " translating the error to EIO for upper layer"
                                << dendl;
                        ioc->set_return_value(-EIO);
                    } else {
                        assert(0 == "got unexpected error from aio_t::get_return_value. "
                        "This may suggest HW issue. Please check your dmesg!");
                    }
                } else if (aio_s[i]->length != (uint64_t)ret) {
                    derr << "aio to " << aio_s[i]->offset << "~" << aio_s[i]->length
                            << " but returned: " << ret << dendl;
                    assert(0 == "unexpected aio error");
                }

                dout(10) << __func__ << " finished aio " << aio_s[i] << " r " << ret
                        << " ioc " << ioc
                        << " with " << (ioc->num_running.load() - 1)
                        << " aios left" << dendl;

                // NOTE: once num_running and we either call the callback or
                // call aio_wake we cannot touch ioc or aio[] as the caller
                // may free it.
                if (ioc->priv) {
                    if (--ioc->num_running == 0) {
                        aio_callback(aio_callback_priv, ioc->priv);
                    }
                } else if (ioc->read_context) {
                    if (--ioc->num_running == 0) {
                        ioc->read_context->thread_id = idx;
                        ioc->read_context->complete_without_del(ioc->get_return_value());
                        if(ioc) {
                            delete ioc;
                        } else {
                            dout(10) << __func__ << " ioc is null" << dendl;
                        }
                    }
                } else {
                    ioc->try_aio_wake();
                }
            }
        }
    }
    dout(10) << __func__ << " end" << dendl;
}

void KernelDevice::aio_submit(IOContext *ioc, bool fixed_thread)
{
    dout(20) << __func__ << " ioc " << ioc
        << " pending " << ioc->num_pending.load()
        << " running " << ioc->num_running.load()
        << dendl;

    if (ioc->num_pending.load() == 0) {
        return;
    }

    // move these aside, and get our end iterator position now, as the
    // aios might complete as soon as they are submitted and queue more
    // wal aio's.
    std::list<aio_t>::iterator e = ioc->running_aios.begin();
    ioc->running_aios.splice(e, ioc->pending_aios);

    int pending = ioc->num_pending.load();
    ioc->num_running += pending;
    ioc->num_pending -= pending;
    assert(ioc->num_pending.load() == 0);  // we should be only thread doing this
    assert(ioc->pending_aios.size() == 0);

    void *priv = static_cast<void*>(ioc);
    int r, retries = 0;
    if (fixed_thread) {
        r = aio_queues[0].submit_batch(ioc->running_aios.begin(), e,
                                   ioc->num_running.load(), priv, &retries);
    } else {
        int idx = ioc->thread_idx;
        assert(idx >= 0 && idx < thread_num);
        r = aio_queues[idx].submit_batch(ioc->running_aios.begin(), e,
                                       ioc->num_running.load(), priv, &retries);
    }

    if (retries)
        derr << __func__ << " retries " << retries << dendl;
    if (r < 0) {
        derr << " aio submit got " << cpp_strerror(r) << dendl;
        assert(r == 0);
    }
}

int KernelDevice::_sync_write(uint64_t off, bufferlist &bl, bool buffered)
{
    uint64_t len = bl.length();
    dout(5) << __func__ << " 0x" << std::hex << off << "~" << len
        << std::dec << " buffered" << dendl;

    std::vector<iovec> iov;
    bl.prepare_iov(&iov);
    int r = ::pwritev(buffered ? fd_buffereds[0] : fd_directs[0],
                &iov[0], iov.size(), off);

    if (r < 0) {
        r = -errno;
        derr << __func__ << " pwritev error: " << cpp_strerror(r) << dendl;
        return r;
    }
    if (buffered) {
        // initiate IO and wait till it completes
        r = ::sync_file_range(fd_buffereds[0], off, len, SYNC_FILE_RANGE_WRITE|SYNC_FILE_RANGE_WAIT_AFTER|SYNC_FILE_RANGE_WAIT_BEFORE);
        if (r < 0) {
            r = -errno;
            derr << __func__ << " sync_file_range error: " << cpp_strerror(r) << dendl;
            return r;
        }
    }

    io_since_flush.store(true);

    return 0;
}

int KernelDevice::write(
  uint64_t off,
  bufferlist &bl,
  bool buffered)
{
    uint64_t len = bl.length();
    dout(20) << __func__ << " 0x" << std::hex << off << "~" << len << std::dec
        << (buffered ? " (buffered)" : " (direct)")
        << dendl;
    assert(off % block_size == 0);
    assert(len % block_size == 0);
    assert(len > 0);
    assert(off < size);
    assert(off + len <= size);

    if ((!buffered || bl.size() >= IOV_MAX) &&
        bl.rebuild_aligned_size_and_memory(block_size)) {
        dout(20) << __func__ << " rebuilding buffer to be aligned" << dendl;
    }

    return _sync_write(off, bl, buffered);
}

int KernelDevice::aio_write(
    uint64_t off,
    bufferlist &bl,
    IOContext *ioc,
    bool buffered)
{
    uint64_t len = bl.length();
    dout(20) << __func__ << " 0x" << std::hex << off << "~" << len << std::dec
        << (buffered ? " (buffered)" : " (direct)")
        << dendl;
    assert(off % block_size == 0);
    assert(len % block_size == 0);
    assert(len > 0);
    assert(off < size);
    assert(off + len <= size);

    if ((!buffered || bl.size() >= IOV_MAX) &&
        bl.rebuild_aligned_size_and_memory(block_size)) {
        dout(20) << __func__ << " rebuilding buffer to be aligned" << dendl;
    }

#ifdef HAVE_LIBAIO
    if (aio && dio && !buffered) {
        if (bl.length() <= RW_IO_MAX) {
            // fast path (non-huge write)
            ioc->pending_aios.push_back(aio_t(ioc, fd_directs[0]));
            ++ioc->num_pending;
            auto& aio_s = ioc->pending_aios.back();
            bl.prepare_iov(&aio_s.iov);
            aio_s.bl.append(bl);
            aio_s.pwritev(off, len);
            dout(30) << aio_s << dendl;
            dout(5) << __func__ << " 0x" << std::hex << off << "~" << len
                << std::dec << " aio " << &aio_s << dendl;
        } else {
            //TODO
            return ENOTSUP;
        }
    } else
#endif
    {
        int r = _sync_write(off, bl, buffered);
        if (r < 0)
            return r;
    }
    return 0;
}

int KernelDevice::read(uint64_t off, uint64_t len, bufferlist *pbl,
		      IOContext *ioc,
		      bool buffered)
{
    dout(5) << __func__ << " 0x" << std::hex << off << "~" << len << std::dec
        << (buffered ? " (buffered)" : " (direct)")
        << dendl;
    assert(off % block_size == 0);
    assert(len % block_size == 0);
    assert(len > 0);
    assert(off < size);
    assert(off + len <= size);

    int r;
    char* p = (char*)aligned_malloc(len, block_size);
    if (!p) {
       r = -errno;
       derr << __func__ << " aligned_malloc failed!" << dendl;
       goto out;
    }
    r = ::pread(buffered ? fd_buffereds[0] : fd_directs[0],
            p, len, off);
    if (r < 0) {
        if (ioc->allow_eio && is_expected_ioerr(r)) {
            r = -EIO;
        } else {
            r = -errno;
        }
        goto out;
    }
    assert((uint64_t)r == len);
    pbl->append(p, len, true, true);

out:
    return r < 0 ? r : 0;
}

int KernelDevice::aio_read(
  uint64_t off,
  uint64_t len,
  bufferlist *pbl,
  IOContext *ioc)
{
    dout(5) << __func__ << " 0x" << std::hex << off << "~" << len << std::dec
        << dendl;

    int r = 0;
#ifdef HAVE_LIBAIO
    if (aio && dio) {
        if (ioc->thread_idx == -1) {
            ioc->thread_idx = cur_thread;
            cur_thread = (cur_thread + 1) % thread_num;
        }
        ioc->pending_aios.push_back(aio_t(ioc, fd_directs[ioc->thread_idx]));
        ++ioc->num_pending;
        aio_t& aio_s = ioc->pending_aios.back();
        aio_s.pread(off, len);
        dout(30) << aio_s << dendl;
        pbl->append(aio_s.bl);
        dout(5) << __func__ << " 0x" << std::hex << off << "~" << len
            << std::dec << " aio " << &aio_s << dendl;
    } else
#endif
    {
        r = read(off, len, pbl, ioc, false);
    }

    return r;
}

int KernelDevice::aio_read(
        uint64_t off,
        uint64_t len,
        char *buf,
        bufferlist *pbl,
        IOContext *ioc)
{
    dout(5) << __func__ << " 0x" << std::hex << off << "~" << len << std::dec
            << dendl;

    int r = 0;
#ifdef HAVE_LIBAIO
    if (aio && dio) {
        if (ioc->thread_idx == -1) {
            ioc->thread_idx = cur_thread;
            cur_thread = (cur_thread + 1) % thread_num;
        }
        ioc->pending_aios.push_back(aio_t(ioc, fd_directs[ioc->thread_idx]));
        ++ioc->num_pending;
        aio_t& aio_s = ioc->pending_aios.back();
        aio_s.pread(off, len, buf);
        dout(30) << aio_s << dendl;
        pbl->append(aio_s.bl);
        dout(5) << __func__ << " 0x" << std::hex << off << "~" << len
            << std::dec << " aio " << &aio_s << dendl;
    } else
#endif
    {
        r = read(off, len, pbl, ioc, false);
    }

    return r;
}

int KernelDevice::direct_read_unaligned(uint64_t off, uint64_t len, char *buf)
{
    int r = 0;
    uint64_t aligned_off = align_down(off, block_size);
    uint64_t aligned_len = align_up(off+len, block_size) - aligned_off;
    char* p = (char*)aligned_malloc(aligned_len, block_size);
    if (!p) {
       r = -errno;
       derr << __func__ << " aligned_malloc failed!" << dendl;
       goto out;
    }

    r = ::pread(fd_directs[0], p, aligned_len, aligned_off);
    if (r < 0) {
        r = -errno;
        derr << __func__ << " 0x" << std::hex << off << "~" << len << std::dec 
        << " error: " << cpp_strerror(r) << dendl;
        goto out;
    }
    assert((uint64_t)r == aligned_len);
    memcpy(buf, p + (off - aligned_off), len);

out:
    aligned_free(p);
    return r < 0 ? r : 0;
}

int KernelDevice::read_random(uint64_t off, uint64_t len, char *buf,
                              bool buffered)
{
    dout(5) << __func__ << " 0x" << std::hex << off << "~" << len << std::dec
            << dendl;
    assert(len > 0);
    assert(off < size);
    assert(off + len <= size);
    int r = 0;

    //if it's direct io and unaligned, we have to use a internal buffer
    if (!buffered && ((off % block_size != 0)
                      || (len % block_size != 0)
                      || (uintptr_t(buf) % block_size != 0)))
        return direct_read_unaligned(off, len, buf);

    if (buffered) {
        //buffered read
        char *t = buf;
        uint64_t left = len;
        while (left > 0) {
            r = ::pread(fd_buffereds[0], t, left, off);
            if (r < 0) {
                r = -errno;
                derr << __func__ << " 0x" << std::hex << off << "~" << left
                     << std::dec << " error: " << cpp_strerror(r) << dendl;
                goto out;
            }
            off += r;
            t += r;
            left -= r;
        }
    } else {
        //direct and aligned read
        r = ::pread(fd_directs[0], buf, len, off);
        if (r < 0) {
            r = -errno;
            derr << __func__ << " direct_aligned_read" << " 0x" << std::hex
                 << off << "~" << len << std::dec << " error: " << cpp_strerror(r)
                 << dendl;
            goto out;
        }
        assert((uint64_t)r == len);
    }
out:
    return r < 0 ? r : 0;
}

int KernelDevice::invalidate_cache(uint64_t off, uint64_t len)
{
    dout(5) << __func__ << " 0x" << std::hex << off << "~" << len << std::dec
        << dendl;
    assert(off % block_size == 0);
    assert(len % block_size == 0);
    int r = posix_fadvise(fd_buffereds[0], off, len, POSIX_FADV_DONTNEED);
    if (r) {
        r = -r;
        derr << __func__ << " 0x" << std::hex << off << "~" << len << std::dec
        << " error: " << cpp_strerror(r) << dendl;
    }
    return r;
}

