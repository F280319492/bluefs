#include <random>
#include <exception>

#include "BlueFS.h"

BlueFS::~BlueFS()
{
    if (ioc) {
        ioc->aio_wait();
    }
    if (bdev) {
        bdev->close();
        delete bdev;
    }
    delete ioc;
}

int BlueFS::add_block_device(const std::string& path)
{
    dout(10) << __func__ << " path " << path << dendl;
    assert(bdev == NULL);
    bdev = BlockDevice::create(cct, path, NULL, NULL);
    int r = bdev->open(path);
    if (r < 0) {
        delete bdev;
        bdev = nullptr;
        return r;
    }
    dout(1) << __func__ << " path " << path
        << " size " << byte_u_t(bdev->get_size()) << dendl;
    ioc = new IOContext(cct, NULL);
    add_block_extent(1024*1024, align_down(bdev->get_size()-1024*1024, bdev->get_block_size()));
    return 0;
}

uint64_t BlueFS::get_block_device_size()
{
    if (bdev)
        return bdev->get_size();
    return 0;
}

void BlueFS::add_block_extent(uint64_t offset, uint64_t length)
{
    std::unique_lock<std::mutex> l(lock);
    dout(1) << __func__ << " 0x" << std::hex << offset << "~" << length << std::dec
        << dendl;
    assert(bdev);
    assert(bdev->get_size() >= offset + length);
    block_all.insert(offset, length);
    block_total += length;

    if (alloc) {
        log_t.op_alloc_add(offset, length);
        int r = _flush_and_sync_log(l);
        assert(r == 0);
        alloc->init_add_free(offset, length);
    }

    dout(10) << __func__ << " done" << dendl;
}

int BlueFS::reclaim_blocks(uint64_t want, PExtentVector *extents)
{
    std::unique_lock<std::mutex> l(lock);
    dout(1) << __func__ << " want 0x" << std::hex << want << std::dec << dendl;
    assert(alloc);

    int64_t got = alloc->allocate(want, alloc_size, 0, extents);
    assert(got != 0);
    if (got < 0) {
        derr << __func__ << " failed to allocate space to return to bluestore"
        << dendl;
        alloc->dump();
        return got;
    }

    for (auto& p : *extents) {
        block_all.erase(p.offset, p.length);
        block_total -= p.length;
        log_t.op_alloc_rm(p.offset, p.length);
    }

    flush_bdev();
    int r = _flush_and_sync_log(l);
    assert(r == 0);

    dout(1) << __func__ << " want 0x" << std::hex << want
        << " got " << extents->size() << dendl;
    return 0;
}

uint64_t BlueFS::get_used()
{
    std::lock_guard<std::mutex> l(lock);
    uint64_t used = 0;
    if (alloc) {
        used = block_all.size() - alloc->get_free();
    }
    return used;
}

uint64_t BlueFS::get_total()
{
    std::lock_guard<std::mutex> l(lock);
    return block_total;
}

uint64_t BlueFS::get_free()
{
    std::lock_guard<std::mutex> l(lock);
    assert(alloc);
    return alloc->get_free();
}

void BlueFS::get_usage(std::pair<uint64_t,uint64_t> *usage)
{
    std::lock_guard<std::mutex> l(lock);
    if (!bdev) {
        (*usage) = std::make_pair(0, 0);
        return;
    }
    (*usage).first = alloc->get_free();
    (*usage).second = block_total;
    uint64_t used = (block_total - (*usage).first) * 100 / block_total;
    dout(10) << __func__ << " free " << (*usage).first
        << " (" << byte_u_t((*usage).first) << ")"
        << " / " << (*usage).second
        << " (" << byte_u_t((*usage).second) << ")"
        << ", used " << used << "%"
        << dendl;
}

void BlueFS::dump_block_extents(std::ostream& out)
{
    if (bdev) {
        out << "size 0x" << std::hex << bdev->get_size()
	        << " : own 0x" << block_all << std::dec << "\n";
    }
}

int BlueFS::mkfs()
{
    std::unique_lock<std::mutex> l(lock);
    dout(1) << __func__ << dendl;

    _init_alloc();

    super.version = 1;
    super.block_size = bdev->get_block_size();
    std::independent_bits_engine<std::default_random_engine, 64, uint64_t> engine;
    super.uuid = engine();
    dout(1) << __func__ << " uuid " << super.uuid << dendl;

    // init log
    FileRef log_file = new File;
    log_file->fnode.ino = 1;
    int r = _allocate(cct->_conf->bluefs_max_log_runway, &log_file->fnode);
    assert(r == 0);
    log_writer = _create_writer(log_file);

    // initial txn
    log_t.op_init();
    interval_set<uint64_t>& p = block_all;
    if (!p.empty()) {
        for (interval_set<uint64_t>::iterator q = p.begin(); q != p.end(); ++q) {
            dout(20) << __func__ << " op_alloc_add 0x" 
                    << std::hex << q.get_start() << "~" << q.get_len() << std::dec
                    << dendl;
            log_t.op_alloc_add(q.get_start(), q.get_len());
        }
    }
    _flush_and_sync_log(l);

    // write supers
    super.log_fnode = log_file->fnode;
    _write_super();
    flush_bdev();

    // clean up
    super = bluefs_super_t();
    _close_writer(log_writer);
    log_writer = NULL;
    block_all.clear();
    block_total = 0;
    _stop_alloc();

    dout(10) << __func__ << " success" << dendl;
    return 0;
}

void BlueFS::_init_alloc()
{
    dout(20) << __func__ << dendl;
    alloc_size = cct->_conf->bluefs_alloc_size;

    if (bdev) {
        assert(bdev->get_size());
        assert(alloc_size);
        std::string name = "bluefs-dev";
        dout(1) << __func__ << " alloc_size 0x" << std::hex << alloc_size
            << " size 0x" << bdev->get_size() << std::dec << dendl;
        alloc = Allocator::create(cct, cct->_conf->bluefs_allocator,
                    bdev->get_size(),
                    alloc_size, name);
        interval_set<uint64_t>& p = block_all;
        for (interval_set<uint64_t>::iterator q = p.begin(); q != p.end(); ++q) {
            alloc->init_add_free(q.get_start(), q.get_len());
        }
    }
}

void BlueFS::_stop_alloc()
{
    dout(20) << __func__ << dendl;
    if (alloc) {
        alloc->shutdown();
        delete alloc;
        alloc = nullptr;
    }
}

int BlueFS::mount()
{
    dout(1) << __func__ << dendl;

    int r = _open_super();
    if (r < 0) {
        derr << __func__ << " failed to open super: " << cpp_strerror(r) << dendl;
        goto out;
    }

    block_all.clear();
    block_total = 0;

    _init_alloc();

    r = _replay(false);
    if (r < 0) {
        derr << __func__ << " failed to replay log: " << cpp_strerror(r) << dendl;
        _stop_alloc();
        goto out;
    }

    // init freelist
    for (auto& p : file_map) {
        dout(30) << __func__ << " noting alloc for " << p.second->fnode << dendl;
        for (auto& q : p.second->fnode.extents) {
            alloc->init_rm_free(q.offset, q.length);
        }
    }

    // set up the log for future writes
    log_writer = _create_writer(_get_file(1));
    assert(log_writer->file->fnode.ino == 1);
    log_writer->pos = log_writer->file->fnode.size;
    dout(10) << __func__ << " log write pos set to 0x"
             << std::hex << log_writer->pos << std::dec
             << dendl;

    return 0;

out:
    super = bluefs_super_t();
    return r;
}

void BlueFS::umount()
{
    dout(1) << __func__ << dendl;

    sync_metadata();

    _close_writer(log_writer);
    log_writer = NULL;

    _stop_alloc();
    file_map.clear();
    dir_map.clear();
    super = bluefs_super_t();
    log_t.clear();
}

void BlueFS::collect_metadata(std::map<std::string,std::string> *pm)
{
    
}

int BlueFS::fsck()
{
    std::lock_guard<std::mutex> l(lock);
    dout(1) << __func__ << dendl;
    // hrm, i think we check everything on mount...
    return 0;
}

int BlueFS::_write_super()
{
    // build superblock
    bufferlist bl;
    super.encode(bl);
    uint32_t crc = bl.crc32c(-1);
    bl.encode_num(&crc, sizeof(crc));
    dout(10) << __func__ << " super block length(encoded): " << bl.length() << dendl;
    dout(10) << __func__ << " superblock " << super.version << dendl;
    dout(10) << __func__ << " log_fnode " << super.log_fnode << dendl;
    assert(bl.length() <= get_super_length());
    bl.append_zero(get_super_length() - bl.length());

    bdev->write(get_super_offset(), bl, false);
    dout(20) << __func__ << " v " << super.version
            << " crc 0x" << std::hex << crc
            << " offset 0x" << get_super_offset() << std::dec
            << dendl;
    return 0;
}

int BlueFS::_open_super()
{
    dout(10) << __func__ << dendl;

    bufferlist bl;
    uint32_t expected_crc, crc;
    int r;

    // always the second block
    r = bdev->read(get_super_offset(), get_super_length(), &bl, ioc, false);
    if (r < 0)
        return r;
    
    super.decode(bl);
    bl.decode_num(&expected_crc, sizeof(expected_crc));
    {
        bufferlist t;
        super.encode(t);
        crc = t.crc32c(-1);
    }
    if (crc != expected_crc) {
        derr << __func__ << " bad crc on superblock, expected 0x"
            << std::hex << expected_crc << " != actual 0x" << crc << std::dec
            << dendl;
        return -EIO;
    }
    dout(10) << __func__ << " superblock " << super.version << dendl;
    dout(10) << __func__ << " log_fnode " << super.log_fnode << dendl;
    return 0;
}

int BlueFS::_replay(bool noop)
{
    dout(10) << __func__ << (noop ? " NO-OP" : "") << dendl;
    ino_last = 1;  // by the log
    log_seq = 0;

    FileRef log_file;
    if (noop) {
        log_file = new File;
    } else {
        log_file = _get_file(1);
    }
    log_file->fnode = super.log_fnode;
    dout(10) << __func__ << " log_fnode " << super.log_fnode << dendl;

    FileReader *log_reader = new FileReader(
            log_file, cct->_conf->bluefs_max_prefetch,
            false,  // !random
            true);  // ignore eof
    while (true) {
        assert((log_reader->buf.pos & ~super.block_mask()) == 0);
        uint64_t pos = log_reader->buf.pos;
        uint64_t read_pos = pos;
        bufferlist bl;
        if (read_pos >= super.log_fnode.get_allocated()) {
            break;
        }

        {
            int r = _read(log_reader, &log_reader->buf, read_pos, super.block_size,
                          &bl, NULL);
            assert(r == (int)super.block_size);
            read_pos += r;
        }
        uint64_t more = 0;
        {
            uint32_t len;
            uint64_t uuid;
            uint64_t seq;
            bl.decode_num(&len, sizeof(len));
            bl.decode_num(&uuid, sizeof(uuid));
            bl.decode_num(&seq, sizeof(seq));
            if (len > bl.length()) {
                more = ROUND_UP_TO(len - bl.length(), super.block_size);
            }
        }

        if (uuid != super.uuid) {
            dout(10) << __func__ << " 0x" << std::hex << pos << std::dec
                     << ": stop: uuid " << uuid << " != super.uuid " << super.uuid
                     << dendl;
            break;
        }
        if (seq != log_seq + 1) {
            dout(10) << __func__ << " 0x" << std::hex << pos << std::dec
                     << ": stop: seq " << seq << " != expected " << log_seq + 1
                     << dendl;
            break;
        }
        if (more) {
            dout(20) << __func__ << " need 0x" << std::hex << more << std::dec
                     << " more bytes" << dendl;
            bufferlist t;
            int r = _read(log_reader, &log_reader->buf, read_pos, more, &t, NULL);
            if (r < (int)more) {
                dout(10) << __func__ << " 0x" << std::hex << pos
                         << ": stop: len is 0x" << bl.length() + more << std::dec
                         << ", which is past eof" << dendl;
                break;
            }
            assert(r == (int)more);
            bl.append(t);
            read_pos += r;
        }

        bluefs_transaction_t t;
        try {
            t.decode(bl);
        }
        catch (std::exception e) {
            dout(10) << __func__ << " 0x" << std::hex << pos << std::dec
                     << ": stop: failed to decode: " << e.what()
                     << dendl;
            delete log_reader;
            return -EIO;
        }
        assert(t.uuid == super.uuid);
        assert(t.seq == log_seq + 1);

        dout(10) << __func__ << " 0x" << std::hex << pos << std::dec
                 << ": " << t << dendl;

        while (!t.op_bl.end()) {
            __u8 op;
            t.op_bl.decode_num(&op, sizeof(op));
            switch (op) {

                case bluefs_transaction_t::OP_INIT:
                    dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                             << ":  op_init" << dendl;
                    assert(t.seq == 1);
                    break;

                case bluefs_transaction_t::OP_JUMP:
                {
                    uint64_t next_seq;
                    uint64_t offset;
                    t.op_bl.decode_num(&next_seq, sizeof(next_seq));
                    t.op_bl.decode_num(&offset, sizeof(offset));
                    dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                             << ":  op_jump seq " << next_seq
                             << " offset 0x" << std::hex << offset << std::dec << dendl;
                    assert(next_seq >= log_seq);
                    log_seq = next_seq - 1; // we will increment it below
                    uint64_t skip = offset - read_pos;
                    if (skip) {
                        bufferlist junk;
                        int r = _read(log_reader, &log_reader->buf, read_pos, skip, &junk,
                                      NULL);
                        if (r != (int)skip) {
                            dout(10) << __func__ << " 0x" << std::hex << read_pos
                                     << ": stop: failed to skip to " << offset
                                     << std::dec << dendl;
                            assert(0 == "problem with op_jump");
                        }
                    }
                }
                    break;

                case bluefs_transaction_t::OP_JUMP_SEQ:
                {
                    uint64_t next_seq;
                    t.op_bl.decode_num(&next_seq, sizeof(next_seq));
                    dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                             << ":  op_jump_seq " << next_seq << dendl;
                    assert(next_seq >= log_seq);
                    log_seq = next_seq - 1; // we will increment it below
                }
                    break;

                case bluefs_transaction_t::OP_ALLOC_ADD:
                {
                    uint64_t offset, length;
                    t.op_bl.decode_num(&offset, sizeof(offset));
                    t.op_bl.decode_num(&length, sizeof(length));
                    dout(20) << __func__ << " 0x" << std::hex << pos << ":  op_alloc_add "
                             << ":0x" << offset << "~" << length << std::dec
                             << dendl;
                    if (!noop) {
                        block_all.insert(offset, length);
                        block_total += length;
                        alloc->init_add_free(offset, length);
                    }
                }
                    break;

                case bluefs_transaction_t::OP_ALLOC_RM:
                {
                    uint64_t offset, length;
                    t.op_bl.decode_num(&offset, sizeof(offset));
                    t.op_bl.decode_num(&length, sizeof(length));
                    dout(20) << __func__ << " 0x" << std::hex << pos
                             << ":  op_alloc_rm :0x" << offset << "~" << length << std::dec
                             << dendl;
                    if (!noop) {
                        block_all.erase(offset, length);
                        block_total -= length;
                        alloc->init_rm_free(offset, length);
                    }
                }
                    break;

                case bluefs_transaction_t::OP_DIR_LINK:
                {
                    std::string dirname, filename;
                    uint64_t ino;
                    t.op_bl.decode_str(&dirname);
                    t.op_bl.decode_str(&filename);
                    t.op_bl.decode_num(&ino, sizeof(ino));
                    dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                             << ":  op_dir_link " << dirname << "/" << filename
                             << " to " << ino
                             << dendl;
                    if (!noop) {
                        FileRef file = _get_file(ino);
                        assert(file->fnode.ino);
                        std::map<std::string,DirRef>::iterator q = dir_map.find(dirname);
                        assert(q != dir_map.end());
                        std::map<std::string,FileRef>::iterator r = q->second->file_map.find(filename);
                        assert(r == q->second->file_map.end());
                        q->second->file_map[filename] = file;
                        ++file->refs;
                    }
                }
                    break;

                case bluefs_transaction_t::OP_DIR_UNLINK:
                {
                    std::string dirname, filename;
                    t.op_bl.decode_str(&dirname);
                    t.op_bl.decode_str(&filename);
                    dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                             << ":  op_dir_unlink " << " " << dirname << "/" << filename
                             << dendl;
                    if (!noop) {
                        std::map<std::string,DirRef>::iterator q = dir_map.find(dirname);
                        assert(q != dir_map.end());
                        std::map<std::string,FileRef>::iterator r = q->second->file_map.find(filename);
                        assert(r != q->second->file_map.end());
                        assert(r->second->refs > 0);
                        --r->second->refs;
                        q->second->file_map.erase(r);
                    }
                }
                    break;

                case bluefs_transaction_t::OP_DIR_CREATE:
                {
                    std::string dirname;
                    t.op_bl.decode_str(&dirname);
                    dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                             << ":  op_dir_create " << dirname << dendl;
                    if (!noop) {
                        std::map<std::string,DirRef>::iterator q = dir_map.find(dirname);
                        assert(q == dir_map.end());
                        dir_map[dirname] = new Dir;
                    }
                }
                    break;

                case bluefs_transaction_t::OP_DIR_REMOVE:
                {
                    std::string dirname;
                    t.op_bl.decode_str(&dirname);
                    dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                             << ":  op_dir_remove " << dirname << dendl;
                    if (!noop) {
                        std::map<std::string,DirRef>::iterator q = dir_map.find(dirname);
                        assert(q != dir_map.end());
                        assert(q->second->file_map.empty());
                        dir_map.erase(q);
                    }
                }
                    break;

                case bluefs_transaction_t::OP_FILE_UPDATE:
                {
                    bluefs_fnode_t fnode;
                    fnode.decode(t.op_bl);
                    dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                             << ":  op_file_update " << " " << fnode << dendl;
                    if (!noop) {
                        FileRef f = _get_file(fnode.ino);
                        f->fnode = fnode;
                        if (fnode.ino > ino_last) {
                            ino_last = fnode.ino;
                        }
                    }
                }
                    break;

                case bluefs_transaction_t::OP_FILE_REMOVE:
                {
                    uint64_t ino;
                    t.op_bl.decode_num(&ino, sizeof(ino));
                    dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                             << ":  op_file_remove " << ino << dendl;
                    if (!noop) {
                        auto p = file_map.find(ino);
                        assert(p != file_map.end());
                        file_map.erase(p);
                    }
                }
                    break;

                default:
                    derr << __func__ << " 0x" << std::hex << pos << std::dec
                         << ": stop: unrecognized op " << (int)op << dendl;
                    delete log_reader;
                    return -EIO;
            }
        }
        assert(t.op_bl.end());

        // we successfully replayed the transaction; bump the seq and log size
        ++log_seq;
        log_file->fnode.size = log_reader->buf.pos;
    }

    dout(10) << __func__ << " log file size was 0x"
             << std::hex << log_file->fnode.size << std::dec << dendl;
    delete log_reader;

    if (!noop) {
        // verify file link counts are all >0
        for (auto& p : file_map) {
            if (p.second->refs == 0 &&
                p.second->fnode.ino > 1) {
                derr << __func__ << " file with link count 0: " << p.second->fnode
                     << dendl;
                return -EIO;
            }
        }
    }

    dout(10) << __func__ << " done" << dendl;
    return 0;
}

BlueFS::FileRef BlueFS::_get_file(uint64_t ino)
{
    auto p = file_map.find(ino);
    if (p == file_map.end()) {
        FileRef f = new File;
        file_map[ino] = f;
        dout(30) << __func__ << " ino " << ino << " = " << f
                 << " (new)" << dendl;
        return f;
    } else {
        dout(30) << __func__ << " ino " << ino << " = " << p->second << dendl;
        return p->second;
    }
}

void BlueFS::_drop_link(FileRef file)
{
    dout(20) << __func__ << " had refs " << file->refs
             << " on " << file->fnode << dendl;
    assert(file->refs > 0);
    --file->refs;
    if (file->refs == 0) {
        dout(20) << __func__ << " destroying " << file->fnode << dendl;
        assert(file->num_reading.load() == 0);
        log_t.op_file_remove(file->fnode.ino);
        for (auto& r : file->fnode.extents) {
            pending_release.insert(r.offset, r.length);
        }
        file_map.erase(file->fnode.ino);
        file->deleted = true;

        if (file->dirty_seq) {
            assert(file->dirty_seq > log_seq_stable);
            assert(dirty_files.count(file->dirty_seq));
            auto it = dirty_files[file->dirty_seq].iterator_to(*file);
            dirty_files[file->dirty_seq].erase(it);
            file->dirty_seq = 0;
        }
    }
}


int BlueFS::_read_random(
        FileReader *h,         ///< [in] read from here
        uint64_t off,          ///< [in] offset
        size_t len,            ///< [in] this many bytes
        char *out)             ///< [out] optional: or copy it here
{
    dout(10) << __func__ << " h " << h
             << " 0x" << std::hex << off << "~" << len << std::dec
             << " from " << h->file->fnode << dendl;

    ++h->file->num_reading;

    if (!h->ignore_eof &&
        off + len > h->file->fnode.size) {
        if (off > h->file->fnode.size)
            len = 0;
        else
            len = h->file->fnode.size - off;
        dout(20) << __func__ << " reaching (or past) eof, len clipped to 0x"
                 << std::hex << len << std::dec << dendl;
    }

    int ret = 0;
    while (len > 0) {
        uint64_t x_off = 0;
        auto p = h->file->fnode.seek(off, &x_off);
        uint64_t l = std::min(p->length - x_off, len);
        dout(20) << __func__ << " read buffered 0x"
                 << std::hex << x_off << "~" << l << std::dec
                 << " of " << *p << dendl;
        int r = bdev->read_random(p->offset + x_off, l, out,
                                           cct->_conf->bluefs_buffered_io);
        assert(r == 0);
        off += l;
        len -= l;
        ret += l;
        out += l;
    }

    dout(20) << __func__ << " got " << ret << dendl;
    --h->file->num_reading;
    return ret;
}

int BlueFS::_read(
        FileReader *h,         ///< [in] read from here
        FileReaderBuffer *buf, ///< [in] reader state
        uint64_t off,          ///< [in] offset
        size_t len,            ///< [in] this many bytes
        bufferlist *outbl,     ///< [out] optional: reference the result here
        char *out)             ///< [out] optional: or copy it here
{
    dout(10) << __func__ << " h " << h
             << " 0x" << std::hex << off << "~" << len << std::dec
             << " from " << h->file->fnode << dendl;

    ++h->file->num_reading;

    if (!h->ignore_eof &&
        off + len > h->file->fnode.size) {
        if (off > h->file->fnode.size)
            len = 0;
        else
            len = h->file->fnode.size - off;
        dout(20) << __func__ << " reaching (or past) eof, len clipped to 0x"
                 << std::hex << len << std::dec << dendl;
    }
    if (outbl)
        outbl->clear();

    int ret = 0;
    while (len > 0) {
        size_t left;
        if (off < buf->bl_off || off >= buf->get_buf_end()) {
            buf->bl.clear();
            buf->bl_off = off & super.block_mask();
            uint64_t x_off = 0;
            auto p = h->file->fnode.seek(buf->bl_off, &x_off);
            uint64_t want = ROUND_UP_TO(len + (off & ~super.block_mask()),
                                        super.block_size);
            want = std::max(want, buf->max_prefetch);
            uint64_t l = std::min(p->length - x_off, want);
            uint64_t eof_offset = ROUND_UP_TO(h->file->fnode.size, super.block_size);
            if (!h->ignore_eof &&
                buf->bl_off + l > eof_offset) {
                l = eof_offset - buf->bl_off;
            }
            dout(20) << __func__ << " fetching 0x"
                     << std::hex << x_off << "~" << l << std::dec
                     << " of " << *p << dendl;
            int r = bdev->read(p->offset + x_off, l, &buf->bl, ioc,
                                        cct->_conf->bluefs_buffered_io);
            assert(r == 0);
        }
        left = buf->get_buf_remaining(off);
        dout(20) << __func__ << " left 0x" << std::hex << left
                 << " len 0x" << len << std::dec << dendl;

        int r = std::min(len, left);
        if (outbl) {
            bufferlist t;
            t.substr_of(buf->bl, off - buf->bl_off, r);
            outbl->append(t);
        }
        if (out) {
            // NOTE: h->bl is normally a contiguous buffer so c_str() is free.
            buf->bl.copy(out, r, off - buf->bl_off);
            out += r;
        }

        dout(30) << __func__ << " result chunk (0x"
                 << std::hex << r << std::dec << " bytes):\n";

        off += r;
        len -= r;
        ret += r;
        buf->pos += r;
    }

    dout(20) << __func__ << " got " << ret << dendl;
    assert(!outbl || (int)outbl->length() == ret);
    --h->file->num_reading;
    return ret;
}

void BlueFS::_invalidate_cache(FileRef f, uint64_t offset, uint64_t length)
{
    dout(10) << __func__ << " file " << f->fnode
             << " 0x" << std::hex << offset << "~" << length << std::dec
             << dendl;
    if (offset & ~super.block_mask()) {
        offset &= super.block_mask();
        length = ROUND_UP_TO(length, super.block_size);
    }
    uint64_t x_off = 0;
    auto p = f->fnode.seek(offset, &x_off);
    while (length > 0 && p != f->fnode.extents.end()) {
        uint64_t x_len = std::min(p->length - x_off, length);
        bdev->invalidate_cache(p->offset + x_off, x_len);
        dout(20) << __func__  << " 0x" << std::hex << x_off << "~" << x_len
                 << std:: dec << " of " << *p << dendl;
        offset += x_len;
        length -= x_len;
    }
}

uint64_t BlueFS::_estimate_log_size()
{
    int avg_dir_size = 40;  // fixme
    int avg_file_size = 12;
    uint64_t size = 4096 * 2;
    size += file_map.size() * (1 + sizeof(bluefs_fnode_t));
    size += block_all.num_intervals() * (1 + 1 + sizeof(uint64_t) * 2);
    size += dir_map.size() + (1 + avg_dir_size);
    size += file_map.size() * (1 + avg_dir_size + avg_file_size);
    return ROUND_UP_TO(size, super.block_size);
}

void BlueFS::compact_log()
{
    std::unique_lock<std::mutex> l(lock);
    if (cct->_conf->bluefs_compact_log_sync) {
        _compact_log_sync();
    } else {
        _compact_log_async(l);
    }
}

bool BlueFS::_should_compact_log()
{
    uint64_t current = log_writer->file->fnode.size;
    uint64_t expected = _estimate_log_size();
    float ratio = (float)current / (float)expected;
    dout(10) << __func__ << " current 0x" << std::hex << current
             << " expected " << expected << std::dec
             << " ratio " << ratio
             << (new_log ? " (async compaction in progress)" : "")
             << dendl;
    if (new_log ||
        current < cct->_conf->bluefs_log_compact_min_size ||
        ratio < cct->_conf->bluefs_log_compact_min_ratio) {
        return false;
    }
    return true;
}


void BlueFS::_compact_log_dump_metadata(bluefs_transaction_t *t)
{
    t->seq = 1;
    t->uuid = super.uuid;
    dout(20) << __func__ << " op_init" << dendl;

    t->op_init();
    for (interval_set<uint64_t>::iterator q = block_all.begin(); q != block_all.end(); ++q) {
        dout(20) << __func__ << " op_alloc_add " << bdev << " 0x"
                 << std::hex << q.get_start() << "~" << q.get_len() << std::dec
                 << dendl;
        t->op_alloc_add(q.get_start(), q.get_len());
    }
    for (auto& p : file_map) {
        if (p.first == 1)
            continue;
        dout(20) << __func__ << " op_file_update " << p.second->fnode << dendl;
        assert(p.first > 1);
        t->op_file_update(p.second->fnode);
    }
    for (auto& p : dir_map) {
        dout(20) << __func__ << " op_dir_create " << p.first << dendl;
        t->op_dir_create(p.first);
        for (auto& q : p.second->file_map) {
            dout(20) << __func__ << " op_dir_link " << p.first << "/" << q.first
                     << " to " << q.second->fnode.ino << dendl;
            t->op_dir_link(p.first, q.first, q.second->fnode.ino);
        }
    }
}

void BlueFS::_compact_log_sync()
{
    dout(10) << __func__ << dendl;
    File *log_file = log_writer->file.get();

    // clear out log (be careful who calls us!!!)
    log_t.clear();

    bluefs_transaction_t t;
    _compact_log_dump_metadata(&t);

    dout(20) << __func__ << " op_jump_seq " << log_seq << dendl;
    t.op_jump_seq(log_seq);

    bufferlist bl;
    t.decode(bl);
    _pad_bl(bl);

    uint64_t need = bl.length() + cct->_conf->bluefs_max_log_runway;
    dout(20) << __func__ << " need " << need << dendl;

    std::vector<bluefs_extent_t> old_extents;
    uint64_t old_allocated = 0;
    log_file->fnode.swap_extents(old_extents, old_allocated);
    while (log_file->fnode.get_allocated() < need) {
        int r = _allocate(need - log_file->fnode.get_allocated(),
                          &log_file->fnode);
        assert(r == 0);
    }

    _close_writer(log_writer);

    log_file->fnode.size = bl.length();
    log_writer = _create_writer(log_file);
    log_writer->append(bl);
    int r = _flush(log_writer, true);
    assert(r == 0);
    wait_for_aio(log_writer);

    std::list<aio_t> completed_ios;
    _claim_completed_aios(log_writer, &completed_ios);
    flush_bdev();
    completed_ios.clear();

    dout(10) << __func__ << " writing super" << dendl;
    super.log_fnode = log_file->fnode;
    ++super.version;
    _write_super();
    flush_bdev();

    dout(10) << __func__ << " release old log extents " << old_extents.size() << dendl;
    for (auto& r : old_extents) {
        pending_release.insert(r.offset, r.length);
    }
}


/*
 * 1. Allocate a new extent to continue the log, and then log an event
 * that jumps the log write position to the new extent.  At this point, the
 * old extent(s) won't be written to, and reflect everything to compact.
 * New events will be written to the new region that we'll keep.
 *
 * 2. While still holding the lock, encode a bufferlist that dumps all of the
 * in-memory fnodes and names.  This will become the new beginning of the
 * log.  The last event will jump to the log continuation extent from #1.
 *
 * 3. Queue a write to a new extent for the new beginnging of the log.
 *
 * 4. Drop lock and wait
 *
 * 5. Retake the lock.
 *
 * 6. Update the log_fnode to splice in the new beginning.
 *
 * 7. Write the new superblock.
 *
 * 8. Release the old log space.  Clean up.
 */
void BlueFS::_compact_log_async(std::unique_lock<std::mutex>& l)
{
    dout(10) << __func__ << dendl;
    File *log_file = log_writer->file.get();
    assert(!new_log);
    assert(!new_log_writer);

    // create a new log [writer] so that we know compaction is in progress
    // (see _should_compact_log)
    new_log = new File;
    new_log->fnode.ino = 0;   // so that _flush_range won't try to log the fnode

    // 0. wait for any racing flushes to complete.  (We do not want to block
    // in _flush_sync_log with jump_to set or else a racing thread might flush
    // our entries and our jump_to update won't be correct.)
    while (log_flushing) {
        dout(10) << __func__ << " log is currently flushing, waiting" << dendl;
        log_cond.wait(l);
    }

    // 1. allocate new log space and jump to it.
    old_log_jump_to = log_file->fnode.get_allocated();
    uint64_t need = old_log_jump_to + cct->_conf->bluefs_max_log_runway;
    dout(10) << __func__ << " old_log_jump_to 0x" << std::hex << old_log_jump_to
             << " need 0x" << need << std::dec << dendl;
    while (log_file->fnode.get_allocated() < need) {
        int r = _allocate(cct->_conf->bluefs_max_log_runway,
                          &log_file->fnode);
        assert(r == 0);
    }
    dout(10) << __func__ << " log extents " << log_file->fnode.extents.size() << dendl;

    // update the log file change and log a jump to the offset where we want to
    // write the new entries
    log_t.op_file_update(log_file->fnode);
    log_t.op_jump(log_seq, old_log_jump_to);

    flush_bdev();  // FIXME?

    _flush_and_sync_log(l, 0, old_log_jump_to);

    // 2. prepare compacted log
    bluefs_transaction_t t;
    //avoid record two times in log_t and _compact_log_dump_metadata.
    log_t.clear();
    _compact_log_dump_metadata(&t);

    uint64_t max_alloc_size = alloc_size;

    // conservative estimate for final encoded size
    new_log_jump_to = ROUND_UP_TO(t.op_bl.length() + super.block_size * 2,
                                  max_alloc_size);
    t.op_jump(log_seq, new_log_jump_to);

    bufferlist bl;
    t.encode(bl);
    _pad_bl(bl);

    dout(10) << __func__ << " new_log_jump_to 0x" << std::hex << new_log_jump_to
             << std::dec << dendl;

    // allocate
    int r = _allocate(new_log_jump_to, &new_log->fnode);
    assert(r == 0);
    new_log_writer = _create_writer(new_log);
    new_log_writer->append(bl);

    // 3. flush
    r = _flush(new_log_writer, true);
    assert(r == 0);
    lock.unlock();

    // 4. wait
    dout(10) << __func__ << " waiting for compacted log to sync" << dendl;
    wait_for_aio(new_log_writer);

    std::list<aio_t> completed_ios;
    _claim_completed_aios(new_log_writer, &completed_ios);
    flush_bdev();
    completed_ios.clear();

    // 5. retake lock
    lock.lock();

    // 6. update our log fnode
    // discard first old_log_jump_to extents
    dout(10) << __func__ << " remove 0x" << std::hex << old_log_jump_to << std::dec
             << " of " << log_file->fnode.extents.size() << dendl;
    uint64_t discarded = 0;
    std::vector<bluefs_extent_t> old_extents;
    while (discarded < old_log_jump_to) {
        assert(!log_file->fnode.extents.empty());
        bluefs_extent_t& e = log_file->fnode.extents.front();
        bluefs_extent_t temp = e;
        if (discarded + e.length <= old_log_jump_to) {
            dout(10) << __func__ << " remove old log extent " << e << dendl;
            discarded += e.length;
            log_file->fnode.pop_front_extent();
        } else {
            dout(10) << __func__ << " remove front of old log extent " << e << dendl;
            uint64_t drop = old_log_jump_to - discarded;
            temp.length = drop;
            e.offset += drop;
            e.length -= drop;
            discarded += drop;
            dout(10) << __func__ << "   kept " << e << " removed " << temp << dendl;
        }
        old_extents.push_back(temp);
    }
    auto from = log_file->fnode.extents.begin();
    auto to = log_file->fnode.extents.end();
    while (from != to) {
        new_log->fnode.append_extent(*from);
        ++from;
    }

    // clear the extents from old log file, they are added to new log
    log_file->fnode.clear_extents();
    // swap the log files. New log file is the log file now.
    new_log->fnode.swap_extents(log_file->fnode);

    // 7. write the super block to reflect the changes
    dout(10) << __func__ << " writing super" << dendl;
    super.log_fnode = log_file->fnode;
    ++super.version;
    _write_super();

    lock.unlock();
    flush_bdev();
    lock.lock();

    // 8. release old space
    dout(10) << __func__ << " release old log extents " << old_extents.size() << dendl;
    for (auto& r : old_extents) {
        pending_release.insert(r.offset, r.length);
    }

    // delete the new log, remove from the dirty files list
    _close_writer(new_log_writer);
    if (new_log->dirty_seq) {
        assert(dirty_files.count(new_log->dirty_seq));
        auto it = dirty_files[new_log->dirty_seq].iterator_to(*new_log);
        dirty_files[new_log->dirty_seq].erase(it);
    }
    new_log_writer = nullptr;
    new_log = nullptr;
    log_cond.notify_all();

    dout(10) << __func__ << " log extents " << log_file->fnode.extents.size() << dendl;
}

void BlueFS::_pad_bl(bufferlist& bl)
{
    uint64_t partial = bl.length() % super.block_size;
    if (partial) {
        dout(10) << __func__ << " padding with 0x" << std::hex
                 << super.block_size - partial << " zeros" << std::dec << dendl;
        bl.append_zero(super.block_size - partial);
    }
}

void BlueFS::flush_log()
{
    std::unique_lock<std::mutex> l(lock);
    flush_bdev();
    _flush_and_sync_log(l);
}

int BlueFS::_flush_and_sync_log(std::unique_lock<std::mutex>& l,
                                uint64_t want_seq,
                                uint64_t jump_to)
{
    while (log_flushing) {
        dout(10) << __func__ << " want_seq " << want_seq
                 << " log is currently flushing, waiting" << dendl;
        assert(!jump_to);
        log_cond.wait(l);
    }
    if (want_seq && want_seq <= log_seq_stable) {
        dout(10) << __func__ << " want_seq " << want_seq << " <= log_seq_stable "
                 << log_seq_stable << ", done" << dendl;
        assert(!jump_to);
        return 0;
    }
    if (log_t.empty() && dirty_files.empty()) {
        dout(10) << __func__ << " want_seq " << want_seq
                 << " " << log_t << " not dirty, dirty_files empty, no-op" << dendl;
        assert(!jump_to);
        return 0;
    }

    interval_set<uint64_t> to_release;
    to_release.swap(pending_release);

    uint64_t seq = log_t.seq = ++log_seq;
    assert(want_seq == 0 || want_seq <= seq);
    log_t.uuid = super.uuid;

    // log dirty files
    auto lsi = dirty_files.find(seq);
    if (lsi != dirty_files.end()) {
        dout(20) << __func__ << " " << lsi->second.size() << " dirty_files" << dendl;
        for (auto &f : lsi->second) {
            dout(20) << __func__ << "   op_file_update " << f.fnode << dendl;
            log_t.op_file_update(f.fnode);
        }
    }

    dout(10) << __func__ << " " << log_t << dendl;
    assert(!log_t.empty());

    // allocate some more space (before we run out)?
    int64_t runway = log_writer->file->fnode.get_allocated() -
                     log_writer->get_effective_write_pos();
    if (runway < (int64_t)cct->_conf->bluefs_min_log_runway) {
        dout(10) << __func__ << " allocating more log runway (0x"
                 << std::hex << runway << std::dec  << " remaining)" << dendl;
        while (new_log_writer) {
            dout(10) << __func__ << " waiting for async compaction" << dendl;
            log_cond.wait(l);
        }
        int r = _allocate(cct->_conf->bluefs_max_log_runway,
                          &log_writer->file->fnode);
        assert(r == 0);
        log_t.op_file_update(log_writer->file->fnode);
    }

    bufferlist bl;
    log_t.encode(bl);

    // pad to block boundary
    _pad_bl(bl);

    log_writer->append(bl);

    log_t.clear();
    log_t.seq = 0;  // just so debug output is less confusing
    log_flushing = true;

    int r = _flush(log_writer, true);
    assert(r == 0);

    if (jump_to) {
        log_writer->file->fnode.size = jump_to;
    }

    _flush_bdev_safely(log_writer);

    log_flushing = false;
    log_cond.notify_all();

    // clean dirty files
    if (seq > log_seq_stable) {
        log_seq_stable = seq;
        dout(20) << __func__ << " log_seq_stable " << log_seq_stable << dendl;

        auto p = dirty_files.begin();
        while (p != dirty_files.end()) {
            if (p->first > log_seq_stable) {
                dout(20) << __func__ << " done cleaning up dirty files" << dendl;
                break;
            }

            auto l = p->second.begin();
            while (l != p->second.end()) {
                File *file = &*l;
                assert(file->dirty_seq > 0);
                assert(file->dirty_seq <= log_seq_stable);
                dout(20) << __func__ << " cleaned file " << file->fnode << dendl;
                file->dirty_seq = 0;
                p->second.erase(l++);
            }

            assert(p->second.empty());
            dirty_files.erase(p++);
        }
    } else {
        dout(20) << __func__ << " log_seq_stable " << log_seq_stable
                 << " already >= out seq " << seq
                 << ", we lost a race against another log flush, done" << dendl;
    }

    if (!to_release.empty()) {
        /* OK, now we have the guarantee alloc[i] won't be null. */
        alloc->release(to_release);
    }

    return 0;
}

int BlueFS::_flush_all(FileWriter *h)
{
    uint64_t offset = h->pos;
    uint64_t length = h->buffer.length();
    dout(10) << __func__ << " " << h << std::hex
             << " 0x" << offset << "~" << length << std::dec
             << " to " << h->file->fnode << dendl;
    assert(!h->file->deleted);
    assert(h->file->num_readers.load() == 0);

    bool buffered;
    if (h->file->fnode.ino == 1)
        buffered = false;
    else
        buffered = cct->_conf->bluefs_buffered_io;

    assert(offset <= h->file->fnode.size);

    uint64_t allocated = h->file->fnode.get_allocated();

    // do not bother to dirty the file if we are overwriting
    // previously allocated extents.
    bool must_dirty = false;
    if (allocated < offset + length) {
        // we should never run out of log space here; see the min runway check
        // in _flush_and_sync_log.
        assert(h->file->fnode.ino != 1);
        int r = _allocate(offset + length - allocated,
                          &h->file->fnode);
        if (r < 0) {
            derr << __func__ << " allocated: 0x" << std::hex << allocated
                 << " offset: 0x" << offset << " length: 0x" << length << std::dec
                 << dendl;
            assert(0 == "bluefs enospc");
            return r;
        }
        if (cct->_conf->bluefs_preextend_wal_files &&
            h->writer_type == WRITER_WAL) {
            // NOTE: this *requires* that rocksdb also has log recycling
            // enabled and is therefore doing robust CRCs on the log
            // records.  otherwise, we will fail to reply the rocksdb log
            // properly due to garbage on the device.
            h->file->fnode.size = h->file->fnode.get_allocated();
            dout(10) << __func__ << " extending WAL size to 0x" << std::hex
                     << h->file->fnode.size << std::dec << " to include allocated"
                     << dendl;
        }
        must_dirty = true;
    }
    if (h->file->fnode.size < offset + length) {
        h->file->fnode.size = offset + length;
        if (h->file->fnode.ino > 1) {
            // we do not need to dirty the log file (or it's compacting
            // replacement) when the file size changes because replay is
            // smart enough to discover it on its own.
            must_dirty = true;
        }
    }
    if (must_dirty) {
        h->file->fnode.mtime = clock_now();
        assert(h->file->fnode.ino >= 1);
        if (h->file->dirty_seq == 0) {
            h->file->dirty_seq = log_seq + 1;
            dirty_files[h->file->dirty_seq].push_back(*h->file);
            dout(20) << __func__ << " dirty_seq = " << log_seq + 1
                     << " (was clean)" << dendl;
        } else {
            if (h->file->dirty_seq != log_seq + 1) {
                // need re-dirty, erase from list first
                assert(dirty_files.count(h->file->dirty_seq));
                auto it = dirty_files[h->file->dirty_seq].iterator_to(*h->file);
                dirty_files[h->file->dirty_seq].erase(it);
                h->file->dirty_seq = log_seq + 1;
                dirty_files[h->file->dirty_seq].push_back(*h->file);
                dout(20) << __func__ << " dirty_seq = " << log_seq + 1
                         << " (was " << h->file->dirty_seq << ")" << dendl;
            } else {
                dout(20) << __func__ << " dirty_seq = " << log_seq + 1
                         << " (unchanged, do nothing) " << dendl;
            }
        }
    }
    dout(20) << __func__ << " file now " << h->file->fnode << dendl;

    uint64_t x_off = 0;
    auto p = h->file->fnode.seek(offset, &x_off);
    assert(p != h->file->fnode.extents.end());
    dout(20) << __func__ << " in " << *p << " x_off 0x"
             << std::hex << x_off << std::dec << dendl;

    bufferlist bl;
    bl.append(h->buffer);

    assert(bl.length() == length);

    uint64_t bloff = 0;
    while (length > 0) {
        uint64_t x_len = std::min(p->length - x_off, length);
        bufferlist t;
        t.substr_of(bl, bloff, x_len);
        unsigned tail = x_len & ~super.block_mask();
        if (tail) {
            size_t zlen = super.block_size - tail;
            dout(20) << __func__ << " caching tail of 0x"
                     << std::hex << tail
                     << " and padding block with 0x" << zlen
                     << std::dec << dendl;

            t.append_zero(zlen);
        }
        if (cct->_conf->bluefs_sync_write) {
            bdev->write(p->offset + x_off, t, buffered);
        } else {
            bdev->aio_write(p->offset + x_off, t, h->iocv, buffered);
        }
        bloff += x_len;
        length -= x_len;
        ++p;
        x_off = 0;
    }
    if (bdev) {
        assert(h->iocv);
        if (h->iocv->has_pending_aios()) {
            bdev->aio_submit(h->iocv);
        }
    }
//    dout(20) << __func__ << " h " << h << " pos now 0x"
//             << std::hex << h->pos << std::dec << dendl;
    return 0;
}

// we need to retire old completed aios so they don't stick around in
// memory indefinitely (along with their bufferlist refs).
void BlueFS::_claim_completed_aios(FileWriter *h, std::list<aio_t> *ls)
{
    if (h->iocv) {
        ls->splice(ls->end(), h->iocv->running_aios);
    }
    dout(10) << __func__ << " got " << ls->size() << " aios" << dendl;
}

void BlueFS::wait_for_aio(FileWriter *h)
{
    // NOTE: this is safe to call without a lock, as long as our reference is
    // stable.
    dout(10) << __func__ << " " << h << dendl;
    utime_t start = clock_now();
    if (h->iocv) {
        h->iocv->aio_wait();
    }
    utime_t end = clock_now();
    utime_t dur = end - start;
    dout(10) << __func__ << " " << h << " done in " << dur << dendl;
}

int BlueFS::_flush(FileWriter *h, bool force)
{
    uint64_t length = h->buffer.length();
    uint64_t offset = 0;
    if (!force &&
        length < cct->_conf->bluefs_min_flush_size) {
        dout(10) << __func__ << " " << h << " ignoring, length " << length
                 << " < min_flush_size " << cct->_conf->bluefs_min_flush_size
                 << dendl;
        return 0;
    }
    if (length == 0) {
        dout(10) << __func__ << " " << h << " no dirty data on "
                 << h->file->fnode << dendl;
        return 0;
    }
    dout(10) << __func__ << " " << h << " 0x"
             << std::hex << offset << "~" << length << std::dec
             << " to " << h->file->fnode << dendl;
    return _flush_all(h);
}

int BlueFS::_fsync(FileWriter *h, std::unique_lock<std::mutex>& l)
{
    dout(10) << __func__ << " " << h << " " << h->file->fnode << dendl;
    int r = _flush(h, true);
    if (r < 0)
        return r;
    uint64_t old_dirty_seq = h->file->dirty_seq;

    _flush_bdev_safely(h);

    if (old_dirty_seq) {
        uint64_t s = log_seq;
        dout(20) << __func__ << " file metadata was dirty (" << old_dirty_seq
                 << ") on " << h->file->fnode << ", flushing log" << dendl;
        _flush_and_sync_log(l, old_dirty_seq);
        assert(h->file->dirty_seq == 0 ||  // cleaned
               h->file->dirty_seq > s);    // or redirtied by someone else
    }
    return 0;
}

void BlueFS::_flush_bdev_safely(FileWriter *h)
{
    if (!cct->_conf->bluefs_sync_write) {
        std::list<aio_t> completed_ios;
        _claim_completed_aios(h, &completed_ios);
        lock.unlock();
        wait_for_aio(h);
        completed_ios.clear();
        flush_bdev();
        lock.lock();
    } else {
        lock.unlock();
        flush_bdev();
        lock.lock();
    }
}

void BlueFS::flush_bdev()
{
    // NOTE: this is safe to call without a lock.
    dout(20) << __func__ << dendl;
    if (bdev)
        bdev->flush();
}

int BlueFS::_allocate(uint64_t len, bluefs_fnode_t* node)
{
    dout(10) << __func__ << " len 0x" << std::hex << len << std::dec << dendl;
    int64_t alloc_len = 0;
    PExtentVector extents;
    if (alloc) {
        uint64_t hint = 0;
        if (!node->extents.empty()) {
            hint = node->extents.back().end();
        }
        extents.reserve(4);  // 4 should be (more than) enough for most allocations
        alloc_len = alloc->allocate(ROUND_UP_TO(len, alloc_size),
                                        alloc_size, hint, &extents);
    }
    if (!alloc ||
        alloc_len < 0 ||
        alloc_len < (int64_t)ROUND_UP_TO(len, alloc_size)) {
        if (alloc_len > 0) {
            alloc->release(extents);
        }

        if (bdev)
            derr << __func__ << " failed to allocate 0x" << std::hex << len
                 << ", free 0x" << alloc->get_free() << std::dec << dendl;
        else
            derr << __func__ << " failed to allocate 0x" << std::hex << len
                    << ", dne" << std::dec << dendl;
        if (alloc)
            alloc->dump();
        return -ENOSPC;
    }

    for (auto& p : extents) {
        node->append_extent(bluefs_extent_t(p.offset, p.length));
    }

    return 0;
}

int BlueFS::_preallocate(FileRef f, uint64_t off, uint64_t len)
{
    dout(10) << __func__ << " file " << f->fnode << " 0x"
             << std::hex << off << "~" << len << std::dec << dendl;
    if (f->deleted) {
        dout(10) << __func__ << "  deleted, no-op" << dendl;
        return 0;
    }
    assert(f->fnode.ino > 1);
    uint64_t allocated = f->fnode.get_allocated();
    if (off + len > allocated) {
        uint64_t want = off + len - allocated;
        int r = _allocate(want, &f->fnode);
        if (r < 0)
            return r;
        log_t.op_file_update(f->fnode);
    }
    return 0;
}

void BlueFS::sync_metadata()
{
    std::unique_lock<std::mutex> l(lock);
    if (log_t.empty()) {
        dout(10) << __func__ << " - no pending log events" << dendl;
        return;
    }
    dout(10) << __func__ << dendl;
    utime_t start = clock_now();
    flush_bdev(); // FIXME?
    _flush_and_sync_log(l);
    dout(10) << __func__ << " done in " << (clock_now() - start) << dendl;

    if (_should_compact_log()) {
        if (cct->_conf->bluefs_compact_log_sync) {
            _compact_log_sync();
        } else {
            _compact_log_async(l);
        }
    }

    utime_t end = clock_now();
    utime_t dur = end - start;
    dout(10) << __func__ << " done in " << dur << dendl;
}

int BlueFS::open_for_write(
        const std::string& dirname,
        const std::string& filename,
        FileWriter **h,
        bool overwrite)
{
    std::lock_guard<std::mutex> l(lock);
    dout(10) << __func__ << " " << dirname << "/" << filename << dendl;
    std::map<std::string,DirRef>::iterator p = dir_map.find(dirname);
    DirRef dir;
    if (p == dir_map.end()) {
        // implicitly create the dir
        dout(20) << __func__ << "  dir " << dirname
                 << " does not exist" << dendl;
        return -ENOENT;
    } else {
        dir = p->second;
    }

    FileRef file;
    bool create = false;
    std::map<std::string,FileRef>::iterator q = dir->file_map.find(filename);
    if (q == dir->file_map.end()) {
        if (overwrite) {
            dout(20) << __func__ << " dir " << dirname << " (" << dir
                     << ") file " << filename
                     << " does not exist" << dendl;
            return -ENOENT;
        }
        file = new File;
        file->fnode.ino = ++ino_last;
        file_map[ino_last] = file;
        dir->file_map[filename] = file;
        ++file->refs;
        create = true;
    } else {
        // overwrite existing file?
        file = q->second;
        if (overwrite) {
            dout(20) << __func__ << " dir " << dirname << " (" << dir
                     << ") file " << filename
                     << " already exists, overwrite in place" << dendl;
        } else {
            dout(20) << __func__ << " dir " << dirname << " (" << dir
                     << ") file " << filename
                     << " already exists, truncate + overwrite" << dendl;
            file->fnode.size = 0;
            for (auto& p : file->fnode.extents) {
                pending_release.insert(p.offset, p.length);
            }

            file->fnode.clear_extents();
        }
    }
    assert(file->fnode.ino > 1);

    file->fnode.mtime = clock_now();

    log_t.op_file_update(file->fnode);
    if (create)
        log_t.op_dir_link(dirname, filename, file->fnode.ino);

    *h = _create_writer(file);

    if (boost::algorithm::ends_with(filename, ".log")) {
        (*h)->writer_type = BlueFS::WRITER_WAL;
    } else if (boost::algorithm::ends_with(filename, ".sst")) {
        (*h)->writer_type = BlueFS::WRITER_SST;
    }

    dout(10) << __func__ << " h " << *h << " on " << file->fnode << dendl;
    return 0;
}

BlueFS::FileWriter *BlueFS::_create_writer(FileRef f)
{
    FileWriter *w = new FileWriter(f);
    if (bdev) {
        w->iocv = new IOContext(cct, NULL);
    } else {
        w->iocv = NULL;
    }
    return w;
}

void BlueFS::_close_writer(FileWriter *h)
{
    dout(10) << __func__ << " " << h << " type " << h->writer_type << dendl;
    if (bdev) {
        assert(h->iocv);
        h->iocv->aio_wait();
    }
    delete h;
}

int BlueFS::open_for_read(
        const std::string& dirname,
        const std::string& filename,
        FileReader **h,
        bool random)
{
    std::lock_guard<std::mutex> l(lock);
    dout(10) << __func__ << " " << dirname << "/" << filename
             << (random ? " (random)":" (sequential)") << dendl;
    std::map<std::string,DirRef>::iterator p = dir_map.find(dirname);
    if (p == dir_map.end()) {
        dout(20) << __func__ << " dir " << dirname << " not found" << dendl;
        return -ENOENT;
    }
    DirRef dir = p->second;

    std::map<std::string,FileRef>::iterator q = dir->file_map.find(filename);
    if (q == dir->file_map.end()) {
        dout(20) << __func__ << " dir " << dirname << " (" << dir
                 << ") file " << filename
                 << " not found" << dendl;
        return -ENOENT;
    }
    File *file = q->second.get();

    *h = new FileReader(file, random ? 4096 : cct->_conf->bluefs_max_prefetch,
                        random, false);
    dout(10) << __func__ << " h " << *h << " on " << file->fnode << dendl;
    return 0;
}

int BlueFS::rename(
        const std::string& old_dirname, const std::string& old_filename,
        const std::string& new_dirname, const std::string& new_filename)
{
    std::lock_guard<std::mutex> l(lock);
    dout(10) << __func__ << " " << old_dirname << "/" << old_filename
             << " -> " << new_dirname << "/" << new_filename << dendl;
    std::map<std::string,DirRef>::iterator p = dir_map.find(old_dirname);
    if (p == dir_map.end()) {
        dout(20) << __func__ << " dir " << old_dirname << " not found" << dendl;
        return -ENOENT;
    }
    DirRef old_dir = p->second;
    std::map<std::string,FileRef>::iterator q = old_dir->file_map.find(old_filename);
    if (q == old_dir->file_map.end()) {
        dout(20) << __func__ << " dir " << old_dirname << " (" << old_dir
                 << ") file " << old_filename
                 << " not found" << dendl;
        return -ENOENT;
    }
    FileRef file = q->second;

    p = dir_map.find(new_dirname);
    if (p == dir_map.end()) {
        dout(20) << __func__ << " dir " << new_dirname << " not found" << dendl;
        return -ENOENT;
    }
    DirRef new_dir = p->second;
    q = new_dir->file_map.find(new_filename);
    if (q != new_dir->file_map.end()) {
        dout(20) << __func__ << " dir " << new_dirname << " (" << old_dir
                 << ") file " << new_filename
                 << " already exists, unlinking" << dendl;
        assert(q->second != file);
        log_t.op_dir_unlink(new_dirname, new_filename);
        _drop_link(q->second);
    }

    dout(10) << __func__ << " " << new_dirname << "/" << new_filename << " "
             << " " << file->fnode << dendl;

    new_dir->file_map[new_filename] = file;
    old_dir->file_map.erase(old_filename);

    log_t.op_dir_link(new_dirname, new_filename, file->fnode.ino);
    log_t.op_dir_unlink(old_dirname, old_filename);
    return 0;
}

int BlueFS::mkdir(const std::string& dirname)
{
    std::lock_guard<std::mutex> l(lock);
    dout(10) << __func__ << " " << dirname << dendl;
    std::map<std::string,DirRef>::iterator p = dir_map.find(dirname);
    if (p != dir_map.end()) {
        dout(20) << __func__ << " dir " << dirname << " exists" << dendl;
        return -EEXIST;
    }
    dir_map[dirname] = new Dir;
    log_t.op_dir_create(dirname);
    return 0;
}

int BlueFS::rmdir(const std::string& dirname)
{
    std::lock_guard<std::mutex> l(lock);
    dout(10) << __func__ << " " << dirname << dendl;
    std::map<std::string,DirRef>::iterator p = dir_map.find(dirname);
    if (p == dir_map.end()) {
        dout(20) << __func__ << " dir " << dirname << " does not exist" << dendl;
        return -ENOENT;
    }
    DirRef dir = p->second;
    if (!dir->file_map.empty()) {
        dout(20) << __func__ << " dir " << dirname << " not empty" << dendl;
        return -ENOTEMPTY;
    }
    dir_map.erase(dirname);
    log_t.op_dir_remove(dirname);
    return 0;
}

bool BlueFS::dir_exists(const std::string& dirname)
{
    std::lock_guard<std::mutex> l(lock);
    std::map<std::string,DirRef>::iterator p = dir_map.find(dirname);
    bool exists = p != dir_map.end();
    dout(10) << __func__ << " " << dirname << " = " << (int)exists << dendl;
    return exists;
}

int BlueFS::stat(const std::string& dirname, const std::string& filename,
                 uint64_t *size, utime_t *mtime)
{
    std::lock_guard<std::mutex> l(lock);
    dout(10) << __func__ << " " << dirname << "/" << filename << dendl;
    std::map<std::string,DirRef>::iterator p = dir_map.find(dirname);
    if (p == dir_map.end()) {
        dout(20) << __func__ << " dir " << dirname << " not found" << dendl;
        return -ENOENT;
    }
    DirRef dir = p->second;
    std::map<std::string,FileRef>::iterator q = dir->file_map.find(filename);
    if (q == dir->file_map.end()) {
        dout(20) << __func__ << " dir " << dirname << " (" << dir
                 << ") file " << filename
                 << " not found" << dendl;
        return -ENOENT;
    }
    File *file = q->second.get();
    dout(10) << __func__ << " " << dirname << "/" << filename
             << " " << file->fnode << dendl;
    if (size)
        *size = file->fnode.size;
    if (mtime)
        *mtime = file->fnode.mtime;
    return 0;
}

int BlueFS::lock_file(const std::string& dirname, const std::string& filename,
                      FileLock **plock)
{
    std::lock_guard<std::mutex> l(lock);
    dout(10) << __func__ << " " << dirname << "/" << filename << dendl;
    std::map<std::string,DirRef>::iterator p = dir_map.find(dirname);
    if (p == dir_map.end()) {
        dout(20) << __func__ << " dir " << dirname << " not found" << dendl;
        return -ENOENT;
    }
    DirRef dir = p->second;
    std::map<std::string,FileRef>::iterator q = dir->file_map.find(filename);
    File *file;
    if (q == dir->file_map.end()) {
        dout(20) << __func__ << " dir " << dirname << " (" << dir
                 << ") file " << filename
                 << " not found, creating" << dendl;
        file = new File;
        file->fnode.ino = ++ino_last;
        file->fnode.mtime = clock_now();
        file_map[ino_last] = file;
        dir->file_map[filename] = file;
        ++file->refs;
        log_t.op_file_update(file->fnode);
        log_t.op_dir_link(dirname, filename, file->fnode.ino);
    } else {
        file = q->second.get();
        if (file->locked) {
            dout(10) << __func__ << " already locked" << dendl;
            return -EBUSY;
        }
    }
    file->locked = true;
    *plock = new FileLock(file);
    dout(10) << __func__ << " locked " << file->fnode
             << " with " << *plock << dendl;
    return 0;
}

int BlueFS::unlock_file(FileLock *fl)
{
    std::lock_guard<std::mutex> l(lock);
    dout(10) << __func__ << " " << fl << " on " << fl->file->fnode << dendl;
    assert(fl->file->locked);
    fl->file->locked = false;
    delete fl;
    return 0;
}

int BlueFS::readdir(const std::string& dirname, std::vector<std::string> *ls)
{
    std::lock_guard<std::mutex> l(lock);
    dout(10) << __func__ << " " << dirname << dendl;
    if (dirname.empty()) {
        // list dirs
        ls->reserve(dir_map.size() + 2);
        for (auto& q : dir_map) {
            ls->push_back(q.first);
        }
    } else {
        // list files in dir
        std::map<std::string,DirRef>::iterator p = dir_map.find(dirname);
        if (p == dir_map.end()) {
            dout(20) << __func__ << " dir " << dirname << " not found" << dendl;
            return -ENOENT;
        }
        DirRef dir = p->second;
        ls->reserve(dir->file_map.size() + 2);
        for (auto& q : dir->file_map) {
            ls->push_back(q.first);
        }
    }
    ls->push_back(".");
    ls->push_back("..");
    return 0;
}

int BlueFS::unlink(const std::string& dirname, const std::string& filename)
{
    std::lock_guard<std::mutex> l(lock);
    dout(10) << __func__ << " " << dirname << "/" << filename << dendl;
    std::map<std::string,DirRef>::iterator p = dir_map.find(dirname);
    if (p == dir_map.end()) {
        dout(20) << __func__ << " dir " << dirname << " not found" << dendl;
        return -ENOENT;
    }
    DirRef dir = p->second;
    std::map<std::string,FileRef>::iterator q = dir->file_map.find(filename);
    if (q == dir->file_map.end()) {
        dout(20) << __func__ << " file " << dirname << "/" << filename
                 << " not found" << dendl;
        return -ENOENT;
    }
    FileRef file = q->second;
    if (file->locked) {
        dout(20) << __func__ << " file " << dirname << "/" << filename
                 << " is locked" << dendl;
        return -EBUSY;
    }
    dir->file_map.erase(filename);
    log_t.op_dir_unlink(dirname, filename);
    _drop_link(file);
    return 0;
}