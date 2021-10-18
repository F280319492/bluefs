// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
  *
 * Copyright (C) 2015 XSky <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <libgen.h>
#include <unistd.h>

#include "BlockDevice.h"
#include "KernelDevice.h"
#if defined(HAVE_SPDK)
#include "SPDKDevice.h"
#endif

void IOContext::aio_wait()
{
    std::unique_lock<std::mutex> l(lock);
    // see _aio_thread for waker logic
    while (num_running.load() > 0) {
        dout(10) << __func__ << " " << this
            << " waiting for " << num_running.load() << " aios to complete"
            << dendl;
        cond.wait(l);
    }
    dout(20) << __func__ << " " << this << " done" << dendl;
}

BlockDevice *BlockDevice::create(BlueFSContext* cct, const std::string& path,
				 aio_callback_t cb, void *cbpriv)
{
    std::string type = "kernel";
    char buf[PATH_MAX + 1];
    int r = ::readlink(path.c_str(), buf, sizeof(buf) - 1);
    if (r >= 0) {
        buf[r] = '\0';
        char *bname = ::basename(buf);
        if (strncmp(bname, SPDK_PREFIX, sizeof(SPDK_PREFIX)-1) == 0)
        type = "ust-nvme";
    }

    dout(1) << __func__ << " path " << path << " type " << type << dendl;

    if (type == "kernel") {
        return new KernelDevice(cct, cb, cbpriv);
    }
#if defined(HAVE_SPDK)
    if (type == "ust-nvme") {
        return new SPDKDevice(cct, cb, cbpriv);
    }
#endif

    derr << __func__ << " unknown backend " << type << dendl;
    abort();
  return NULL;
}

int get_block_device_size(int fd, int64_t *psize)
{
    int ret = ::ioctl(fd, BLKGETSIZE64, psize);
    if (ret < 0)
        ret = -errno;
    return ret;
}
