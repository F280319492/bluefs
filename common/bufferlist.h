#ifndef BUFFERLIST_H
#define BUFFERLIST_H

#include <list>
#include <vector>
#include <iostream>
#include <stdlib.h>
#include <algorithm>
#include <assert.h>

#include "debug.h"

#define IOV_MAX		1024
#define ALLOC_SIZE  4096

void* aligned_malloc(size_t required_bytes, size_t alignment);

void aligned_free(void *p2);

template <typename T>
inline constexpr T align_up(T v, T align) {
  return (v + align - 1) & ~(align - 1);
}

template <typename T>
inline constexpr T align_down(T v, T align) {
  return v & ~(align - 1);
}

#ifndef ROUND_UP_TO
#define ROUND_UP_TO(n, d) ((n)%(d) ? ((n)+(d)-(n)%(d)) : (n))
#endif

struct buffernode {
    void       *buf;
    uint32_t    len;
    uint32_t    cap;
    bool        is_align;
    bool        need_free;
    buffernode(void* p, int l, bool align) : buf(p), len(l), cap(l), is_align(align), need_free(false) {}
    buffernode(void* p, int l) : buf(p), len(l), cap(l), is_align(false), need_free(false) {}
    buffernode() : buf(nullptr), len(0), cap(0), is_align(false), need_free(false) {}
    buffernode(const buffernode& node) : buf(node.buf), len(node.len), cap(node.cap), is_align(node.is_align), need_free(node.need_free) {}
    buffernode(void* p, int l, bool align, bool need_free) : buf(p), len(l), cap(l), is_align(align), need_free(need_free) {}
    buffernode(void* p, int l, int cap, bool align) : buf(p), len(l), cap(cap), is_align(align), need_free(false) {}
    buffernode(void* p, int l, int cap, bool align, bool need_free) : buf(p), len(l), cap(cap), is_align(align), need_free(need_free) {}
    buffernode operator=(const buffernode& node) {
        return buffernode(node);
    }
};


using bufferlist_v = std::vector<buffernode>;

class bufferlist {
public:
    bufferlist() : capacity(0), idx(0), off(0) {}
    bufferlist &operator =(const bufferlist & other) {
        clear_free();
        this->encode_bufferlist(other);
    }
    bufferlist(bufferlist &&other) {
        clear_free();
        this->encode_bufferlist(other);
    }
    bufferlist &operator =(bufferlist &&other) {
        clear_free();
        this->encode_bufferlist(other);
    }

    ~bufferlist() {
        clear_free();
    }

    uint32_t crc32c(uint32_t) const;

    void clear_free() {
        while (!bl.empty()) {
            buffernode& node = bl.back();
            if (node.need_free) {
                if (node.is_align) {
                    aligned_free(node.buf);
                } else {
                    free(node.buf);
                }
            }
            bl.pop_back();
        }
        capacity = 0;
    }

    void clear() {
        bl.clear();
        capacity = 0;
    }

    void append(char *buf, size_t len) {
        bl.push_back(buffernode(buf, len));
        capacity += len;
    }

    void append(char *buf, size_t len, bool is_align) {
        bl.push_back(buffernode(buf, len, is_align));
        capacity += len;
    }

    void append(char *buf, size_t len, bool is_align, bool need_free) {
        bl.push_back(buffernode(buf, len, is_align, need_free));
        capacity += len;
    }

    void append(char *buf, size_t len, size_t cap, bool is_align, bool need_free) {
        bl.push_back(buffernode(buf, len, cap, is_align, need_free));
        capacity += len;
    }
    
    void append(bufferlist &src_bl) {
        for (auto& p : src_bl.get_buffer()) {
            bl.push_back(p);
            capacity += p.len;
        }
        src_bl.clear();
    }

    void append_zero(unsigned len) {
        char* buf = (char*)malloc(len);
        if (!buf) {
            derr << __func__ << " malloc failed!" << dendl;
        }
        memset(buf, 0, len);
        append(buf, len, len, false, true);
    }

    void copy(char *buf, size_t len) const {
        if (len > capacity) {
            throw std::range_error("error bufferlist len to copy");
        } else {
            size_t off = 0;
            size_t idx = 0;
            while (len > 0) {
                const buffernode& node = bl[idx];
                size_t copy_len = std::min(len, (size_t)node.len);
                memcpy(buf+off, (uint8_t *)node.buf, copy_len);
                len -= copy_len;
                off += copy_len;
                idx++;
            }
        }
    }

    void copy(char *buf, size_t len, size_t offset) const {
        if (offset + len > capacity) {
            throw std::range_error("error bufferlist len to copy");
        } else {
            size_t length = len;
            size_t off = 0;
            size_t idx = 0;
            while (offset >= bl[idx].len) {
                offset -= bl[idx].len;
                idx++;
            }
            while (length > 0) {
                const buffernode& node = bl[idx];
                size_t copy_len = std::min(length, (size_t)(node.len-offset));
                memcpy(buf+off, (uint8_t *)node.buf+offset, copy_len);
                length -= copy_len;
                off += copy_len;
                idx++;
                offset = 0;
            }
        }
    }

    void copy(void *buf, size_t len) const {
        copy((char*)buf, len);
    }

    void copy(void *buf, size_t len, size_t offset) const {
        copy((char*)buf, len, offset);
    }

    std::size_t size() const {
        return bl.size();
    }

    std::size_t length() const {
        return capacity;
    }

    template<typename VectorT>
    void prepare_iov(VectorT *piov) const {
        assert(bl.size() <= IOV_MAX);
        piov->resize(bl.size());
        unsigned n = 0;
        for (auto& p : bl) {
            (*piov)[n].iov_base = p.buf;
            (*piov)[n].iov_len = p.len;
            ++n;
        }
    }

    const bufferlist_v& get_buffer() {
        return bl;
    }

    bool rebuild_aligned_size_and_memory(unsigned align_size) {
        if (bl.size() > 1 || (bl.size() == 1 && (bl[0].len%align_size || (uint64_t)bl[0].buf%align_size))) {
            buffernode node;
            node.len = capacity;
            node.is_align = true;
            uint32_t align_len = align_up(capacity, align_size);
            node.buf = aligned_malloc(align_len, align_size);
            if (!node.buf) {
                derr << __func__ << " aligned_malloc failed!" << dendl;
                return false;
            }
            copy(node.buf, capacity);
            clear_free();
            bl.push_back(node);
            capacity += node.len;
            return true;
        }
    }
    bool encode(const void* buf, size_t len);
    template <typename T>  
    bool encode_num(const T v);
    bool encode_str(const std::string& str);
    bool encode_bufferlist(const bufferlist& bl);

    bool decode(void* buf, size_t len);
    template <typename T>  
    bool decode_num(T* v);
    bool decode_str(std::string* str);
    bool decode_bufferlist(bufferlist* bl);

    void substr_of(const bufferlist& other, unsigned off, unsigned len) {
        size_t alloc_size = align_up(len, (uint32_t)ALLOC_SIZE);
        void* buf = aligned_malloc(alloc_size, ALLOC_SIZE);
        copy(buf, len, off);
        append((char*)buf, (size_t)len, alloc_size, true, true);
    }

    bool end() {
        if (!bl.size()) {
            return true;
        }
        if (idx == bl.size() - 1 && bl.back().len == off) {
            return true;
        }
        return false;
    }

private:
    bufferlist_v bl;
    uint32_t capacity;
    uint32_t idx;
    uint32_t off;
};

#endif //BUFFERLIST_H