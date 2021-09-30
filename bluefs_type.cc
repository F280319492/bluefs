#include "bluefs_type.h"

void bluefs_extent_t::encode(bufferlist& bl) const {
    bl.encode_num(offset);
    bl.encode_num(length);
}

void bluefs_extent_t::decode(bufferlist& bl) {
    bl.decode_num(&offset);
    bl.decode_num(&length);
}

void bluefs_extent_t::dump() const {
    dout(1) << __func__ << " 0x" << std::hex << offset << "~" << length << std::dec << dendl;
}

std::ostream& operator<<(std::ostream& out, const bluefs_extent_t& o) {
    return out << "0x" << std::hex << o.offset << "~" << o.length << std::dec;
}

void bluefs_fnode_t::encode(bufferlist& bl) const {
    bl.encode_num(ino);
    bl.encode_num(size);
    mtime.encode(bl);
    bl.encode_num(extents.size());
    for (auto& p : extents) {
        p.encode(bl);
    }
}

void bluefs_fnode_t::decode(bufferlist& bl) {
    bl.decode_num(&ino);
    bl.decode_num(&size);
    mtime.decode(bl);
    size_t extents_len;
    bl.decode_num(&extents_len);
    for (size_t i = 0; i < extents_len; i++) {
        extents.push_back(bluefs_extent_t());
        extents.back().decode(bl);
    }
    recalc_allocated();
}

std::vector<bluefs_extent_t>::iterator bluefs_fnode_t::seek(
        uint64_t offset, uint64_t *x_off) {
    auto p = extents.begin();
    while (p != extents.end()) {
        if ((int64_t) offset >= p->length) {
            offset -= p->length;
            ++p;
        } else {
            break;
        }
    }
    *x_off = offset;
    return p;
}

void bluefs_fnode_t::dump() const {
    dout(1) << __func__ << " ino:" << ino << " size:" << size << " mtime:" << mtime << dendl;
    dout(1) << "extents:" << dendl;
    for (auto& p : extents) {
        dout(1) << "\t" << p << dendl;
    }
}

std::ostream& operator<<(std::ostream& out, const bluefs_fnode_t& file) {
    out << " ino:" << file.ino << " size:" << file.size << " mtime:" << file.mtime << std::endl;
    out << "extents:" << std::endl;
    for (auto& p : file.extents) {
        out << "\t" << p << std::endl;
    }
}


void bluefs_super_t::encode(bufferlist& bl) const {
    bl.encode_num(uuid);
    bl.encode_num(version);
    bl.encode_num(block_size);
    log_fnode.encode(bl);
}

void bluefs_super_t::decode(bufferlist& bl) {
    bl.decode_num(&uuid);
    bl.decode_num(&version);
    bl.decode_num(&block_size);
    log_fnode.decode(bl);
}

void ::bluefs_super_t::dump() const {
    dout(1) << __func__ << " super(uuid " << uuid
	     << " v " << version
	     << " block_size 0x" << std::hex << block_size
	     << " log_fnode 0x" << log_fnode
	     << std::dec << ")" << dendl;
}

std::ostream& operator<<(std::ostream& out, const bluefs_super_t& s) {
    return out << "super(uuid " << s.uuid
	     << " v " << s.version
	     << " block_size 0x" << std::hex << s.block_size
	     << " log_fnode 0x" << s.log_fnode
	     << std::dec << ")";
}

void bluefs_transaction_t::encode(bufferlist& bl) const {
    uint32_t crc = op_bl.crc32c(-1);
    bl.encode_num(uuid);
    bl.encode_num(seq);
    bl.encode_num(crc);
    bl.encode_bufferlist(op_bl);
}

void bluefs_transaction_t::decode(bufferlist& bl) {
    uint32_t crc;
    bl.decode_num(&uuid);
    bl.decode_num(&seq);
    bl.decode_num(&crc);
    bl.decode_bufferlist(&op_bl);
    uint32_t actual = op_bl.crc32c(-1);
    if (actual != crc)
    throw std::range_error("error bufferlist len to copy");("bad crc " + std::to_string(actual)
				  + " expected " + std::to_string(crc));
}

void bluefs_transaction_t::dump() const {
    dout(1) << "txn(seq " << seq
	     << " len 0x" << std::hex << op_bl.length()
	     << " crc 0x" << op_bl.crc32c(-1)
	     << std::dec << ")";
}

std::ostream& operator<<(std::ostream& out, const bluefs_transaction_t& t) {
    return out << "txn(seq " << t.seq
	     << " len 0x" << std::hex << t.op_bl.length()
	     << " crc 0x" << t.op_bl.crc32c(-1)
	     << std::dec << ")";
}
