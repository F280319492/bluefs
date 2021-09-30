#include <random>

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

int BlueFS::add_block_device(const string& path)
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
        << " size " << byte_u_t(b->get_size()) << dendl;
    ioc = new IOContext(cct, NULL);
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

    if (logger)
        logger->inc(l_bluefs_reclaim_bytes, got);
    dout(1) << __func__ << " want 0x" << std::hex << want
        << " got " << *extents << dendl;
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
        (*usage) = make_pair(0, 0);
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
    std::independent_bits_engine<default_random_engine, 64, uint64_t> engine;
    super.uuid = engine();
    dout(1) << __func__ << " uuid " << super.uuid << dendl;

    // init log
    FileRef log_file = new File;
    log_file->fnode.ino = 1;
    int r = _allocate(0, cct->_conf->bluefs_max_log_runway, &log_file->fnode);
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
            log_t.op_alloc_add(bdev, q.get_start(), q.get_len());
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
    _stop_alloc();
    _shutdown_logger();

    dout(10) << __func__ << " success" << dendl;
    return 0;
}

void BlueFS::_init_alloc()
{
    dout(20) << __func__ << dendl;

    alloc_size = cct->_conf->bluefs_alloc_size;

    if (!bdev) {
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

void BlueFS::collect_metadata(map<string,string> *pm)
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
    bl.encode_num(crc);
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
    bufferlist::iterator p = bl.begin();
    bl.decode_num(&expected_crc);
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
    
}