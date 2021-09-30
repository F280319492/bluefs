#ifndef BLUEFS_TYPE_H
#define BLUEFS_TYPE_H

#include <iostream>
#include <vector>

#include "common/utime.h"
#include "common/debug.h"
#include "common/bufferlist.h"

class bluefs_extent_t {
public:
    uint64_t offset = 0;
    uint64_t length = 0;

    bluefs_extent_t(uint64_t o = 0, uint64_t l = 0)
        : offset(o), length(l) {}

    uint64_t end() const { return  offset + length; }

    void encode(bufferlist& bl) const;

    void decode(bufferlist& bl);

    void dump() const;
};
std::ostream& operator<<(std::ostream& out, const bluefs_extent_t& e);


struct bluefs_fnode_t {
    uint64_t ino;
    uint64_t size;
    utime_t mtime;
    std::vector<bluefs_extent_t> extents;
    uint64_t allocated;

    bluefs_fnode_t() : ino(0), size(0), allocated(0) {}

    uint64_t get_allocated() const {
        return allocated;
    }

    void recalc_allocated() {
        allocated = 0;
        for (auto& p : extents)
            allocated += p.length;
    }

    void encode(bufferlist& bl) const;

    void decode(bufferlist& bl);

    void append_extent(const bluefs_extent_t& ext) {
        extents.push_back(ext);
        allocated += ext.length;
    }

    void pop_front_extent() {
        auto it = extents.begin();
        allocated -= it->length;
        extents.erase(it);
    }
    
    void swap_extents(bluefs_fnode_t& other) {
        other.extents.swap(extents);
        std::swap(allocated, other.allocated);
    }
    void swap_extents(std::vector<bluefs_extent_t>& swap_to, uint64_t& new_allocated) {
        swap_to.swap(extents);
        std::swap(allocated, new_allocated);
    }
    void clear_extents() {
        extents.clear();
        allocated = 0;
    }

    std::vector<bluefs_extent_t>::iterator seek(
        uint64_t off, uint64_t *x_off);

    void dump() const;

};
std::ostream& operator<<(std::ostream& out, const bluefs_fnode_t& file);


struct bluefs_super_t {
    uint64_t uuid;      ///< unique to this bluefs instance
    uint64_t version;
    uint32_t block_size;

    bluefs_fnode_t log_fnode;

    bluefs_super_t()
        : version(0),
        block_size(4096) { }

    uint64_t block_mask() const {
        return ~((uint64_t)block_size - 1);
    }

    void encode(bufferlist& bl) const;
    void decode(bufferlist& bl);
    void dump() const;
};
std::ostream& operator<<(std::ostream&, const bluefs_super_t& s);


struct bluefs_transaction_t {
    typedef enum {
        OP_NONE = 0,
        OP_INIT,        ///< initial (empty) file system marker
        OP_ALLOC_ADD,   ///< add extent to available block storage (extent)
        OP_ALLOC_RM,    ///< remove extent from availabe block storage (extent)
        OP_DIR_LINK,    ///< (re)set a dir entry (dirname, filename, ino)
        OP_DIR_UNLINK,  ///< remove a dir entry (dirname, filename)
        OP_DIR_CREATE,  ///< create a dir (dirname)
        OP_DIR_REMOVE,  ///< remove a dir (dirname)
        OP_FILE_UPDATE, ///< set/update file metadata (file)
        OP_FILE_REMOVE, ///< remove file (ino)
        OP_JUMP,        ///< jump the seq # and offset
        OP_JUMP_SEQ,    ///< jump the seq #
    } op_t;

    uint64_t uuid;          ///< fs uuid
    uint64_t seq;         ///< sequence number
    bufferlist op_bl;     ///< encoded transaction ops

    bluefs_transaction_t() : seq(0) {}

    void clear() {
        seq = 0;
        op_bl.clear(); //clear_free ??
    }
    bool empty() const {
        return op_bl.length() == 0;
    }

    void op_init() {
        op_bl.encode_num((uint8_t)OP_INIT);
    }
    void op_alloc_add(uint64_t offset, uint64_t length) {
        op_bl.encode_num((uint8_t)OP_ALLOC_ADD);
        op_bl.encode_num(offset);
        op_bl.encode_num(length);
    }
    void op_alloc_rm(uint64_t offset, uint64_t length) {
        op_bl.encode_num((uint8_t)OP_ALLOC_RM);
        op_bl.encode_num(offset);
        op_bl.encode_num(length);
    }
    void op_dir_create(const std::string& dir) {
        op_bl.encode_num((uint8_t)OP_DIR_CREATE);
        op_bl.encode_str(dir);
    }
    void op_dir_remove(const std::string& dir) {
        op_bl.encode_num((uint8_t)OP_DIR_REMOVE);
         op_bl.encode_str(dir);
    }
    void op_dir_link(const std::string& dir, const std::string& file, uint64_t ino) {
        op_bl.encode_num((uint8_t)OP_DIR_LINK);
        op_bl.encode_str(dir);
        op_bl.encode_str(file);
        op_bl.encode_num(ino);
    }
    void op_dir_unlink(const std::string& dir, const std::string& file) {
        op_bl.encode_num((uint8_t)OP_DIR_UNLINK);
        op_bl.encode_str(dir);
        op_bl.encode_str(file);
    }
    void op_file_update(const bluefs_fnode_t& file) {
        op_bl.encode_num((uint8_t)OP_FILE_UPDATE);
        file.encode(op_bl);
    }
    void op_file_remove(uint64_t ino) {
        op_bl.encode_num((uint8_t)OP_FILE_REMOVE);
        op_bl.encode_num(ino);
    }
    void op_jump(uint64_t next_seq, uint64_t offset) {
        op_bl.encode_num((uint8_t)OP_JUMP);
        op_bl.encode_num(next_seq);
        op_bl.encode_num(offset);
    }
    void op_jump_seq(uint64_t next_seq) {
        op_bl.encode_num((uint8_t)OP_JUMP_SEQ);
        op_bl.encode_num(next_seq);
    }

    void encode(bufferlist& bl) const;
    void decode(bufferlist& bl);
    void dump() const;
};
std::ostream& operator<<(std::ostream& out, const bluefs_transaction_t& t);

#endif //BLUEFS_TYPE_H