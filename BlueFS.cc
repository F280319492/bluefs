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
    _stop_alloc();

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
        {
            int r = _read(log_reader, &log_reader->buf, read_pos, super.block_size,
                          &bl, NULL);
            assert(r == (int)super.block_size);
            read_pos += r;
        }
        uint64_t more = 0;
        uint64_t seq;
        uuid_d uuid;
        {
            __u8 a, b;
            uint32_t len;
            bl.decode_num(&a);
            bl.decode_num(&b);
            bl.decode_num(&len);
            bl.decode_num(&uuid);
            bl.decode_num(&seq);
            if (len + 6 > bl.length()) {
                more = ROUND_UP_TO(len + 6 - bl.length(), super.block_size);
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
            bufferlist::iterator p = bl.begin();
            ::decode(t, p);
        }
        catch (buffer::error& e) {
            dout(10) << __func__ << " 0x" << std::hex << pos << std::dec
                     << ": stop: failed to decode: " << e.what()
                     << dendl;
            delete log_reader;
            return -EIO;
        }
        assert(seq == t.seq);
        dout(10) << __func__ << " 0x" << std::hex << pos << std::dec
                 << ": " << t << dendl;

        bufferlist::iterator p = t.op_bl.begin();
        while (!p.end()) {
            __u8 op;
            ::decode(op, p);
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
                    ::decode(next_seq, p);
                    ::decode(offset, p);
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
                    ::decode(next_seq, p);
                    dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                             << ":  op_jump_seq " << next_seq << dendl;
                    assert(next_seq >= log_seq);
                    log_seq = next_seq - 1; // we will increment it below
                }
                    break;

                case bluefs_transaction_t::OP_ALLOC_ADD:
                {
                    __u8 id;
                    uint64_t offset, length;
                    ::decode(id, p);
                    ::decode(offset, p);
                    ::decode(length, p);
                    dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                             << ":  op_alloc_add " << " " << (int)id
                             << ":0x" << std::hex << offset << "~" << length << std::dec
                             << dendl;
                    if (!noop) {
                        block_all[id].insert(offset, length);
                        block_total[id] += length;
                        alloc[id]->init_add_free(offset, length);
                    }
                }
                    break;

                case bluefs_transaction_t::OP_ALLOC_RM:
                {
                    __u8 id;
                    uint64_t offset, length;
                    ::decode(id, p);
                    ::decode(offset, p);
                    ::decode(length, p);
                    dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                             << ":  op_alloc_rm " << " " << (int)id
                             << ":0x" << std::hex << offset << "~" << length << std::dec
                             << dendl;
                    if (!noop) {
                        block_all[id].erase(offset, length);
                        block_total[id] -= length;
                        alloc[id]->init_rm_free(offset, length);
                    }
                }
                    break;

                case bluefs_transaction_t::OP_DIR_LINK:
                {
                    string dirname, filename;
                    uint64_t ino;
                    ::decode(dirname, p);
                    ::decode(filename, p);
                    ::decode(ino, p);
                    dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                             << ":  op_dir_link " << " " << dirname << "/" << filename
                             << " to " << ino
                             << dendl;
                    if (!noop) {
                        FileRef file = _get_file(ino);
                        assert(file->fnode.ino);
                        map<string,DirRef>::iterator q = dir_map.find(dirname);
                        assert(q != dir_map.end());
                        map<string,FileRef>::iterator r = q->second->file_map.find(filename);
                        assert(r == q->second->file_map.end());
                        q->second->file_map[filename] = file;
                        ++file->refs;
                    }
                }
                    break;

                case bluefs_transaction_t::OP_DIR_UNLINK:
                {
                    string dirname, filename;
                    ::decode(dirname, p);
                    ::decode(filename, p);
                    dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                             << ":  op_dir_unlink " << " " << dirname << "/" << filename
                             << dendl;
                    if (!noop) {
                        map<string,DirRef>::iterator q = dir_map.find(dirname);
                        assert(q != dir_map.end());
                        map<string,FileRef>::iterator r = q->second->file_map.find(filename);
                        assert(r != q->second->file_map.end());
                        assert(r->second->refs > 0);
                        --r->second->refs;
                        q->second->file_map.erase(r);
                    }
                }
                    break;

                case bluefs_transaction_t::OP_DIR_CREATE:
                {
                    string dirname;
                    ::decode(dirname, p);
                    dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                             << ":  op_dir_create " << dirname << dendl;
                    if (!noop) {
                        map<string,DirRef>::iterator q = dir_map.find(dirname);
                        assert(q == dir_map.end());
                        dir_map[dirname] = new Dir;
                    }
                }
                    break;

                case bluefs_transaction_t::OP_DIR_REMOVE:
                {
                    string dirname;
                    ::decode(dirname, p);
                    dout(20) << __func__ << " 0x" << std::hex << pos << std::dec
                             << ":  op_dir_remove " << dirname << dendl;
                    if (!noop) {
                        map<string,DirRef>::iterator q = dir_map.find(dirname);
                        assert(q != dir_map.end());
                        assert(q->second->file_map.empty());
                        dir_map.erase(q);
                    }
                }
                    break;

                case bluefs_transaction_t::OP_FILE_UPDATE:
                {
                    bluefs_fnode_t fnode;
                    ::decode(fnode, p);
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
                    ::decode(ino, p);
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
        assert(p.end());

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