#ifndef BUFFERLIST_H
#define BUFFERLIST_H

#include <list>
#include <vector>
#include <iostream>
#include <stdlib.h>

#define IOV_MAX		1024

struct iovec {
	void *   iov_base;      /* [XSI] Base address of I/O memory region */
	size_t   iov_len;       /* [XSI] Size of region iov_base points to */
};

void* aligned_malloc(size_t required_bytes, size_t alignment) {
    uint32_t offset = alignment - 1 + sizeof(void*);
    void* p1 = (void*)malloc(required_bytes + offset);
    if (p1 == NULL)
        return NULL;

    void** p2 = (void**)(((size_t)p1 + offset ) & ~(alignment - 1));
    p2[-1] = p1;

    return p2;
}

void aligned_free(void *p2){
    if (p2) {
        void* p1 = ((void**)p2)[-1];
        free(p1);
    }
}

template <typename T>
inline constexpr T align_up(T v, T align) {
  return (v + align - 1) & ~(align - 1);
}

template <typename T>
inline constexpr T align_down(T v, T align) {
  return v & ~(align - 1);
}


struct buffernode {
    void       *buf;
    uint32_t    len;
    bool        is_align;
    buffernode(const void* p, int l, bool align) : buf(p), len(l), is_align(align) {}
    buffernode(const void* p, int l) : buf(p), len(l), is_align(false) {}
};


using bufferlist_v = std::vector<buffernode>;

class bufferlist {
public:
    bufferlist() : capacity(0) {}
    ~bufferlist() {
        clear_free();
    }

    void clear_free() {
        while (!bl.empty()) {
            buffernode& node = bl.back();
            if (node.is_align) {
                aligned_free(node.buf);
            } else {
                free(node.buf);
            }

            bl.pop_back();
        }
        capacity = 0;
    }

    void clear() {
        bl.clear();
        capacity = 0;
    }

    void append(const char *buf, size_t len) {
        bl.push_back(buffernode(buf, len));
        capacity += len;
    }

    void append(const char *buf, size_t len, bool is_align) {
        bl.push_back(buffernode(buf, len, is_align));
        capacity += len;
    }
    
    void append(const bufferlist &src_bl) {
        for (auto& p : src_bl.get_buffer()) {
            bl.push_back(p);
            capacity += p.len;
        }
        src_bl.clear();
    }

    void copy(char *buf, size_t len) {
        if (len > capacity) {
            throw std::range_error("error bufferlist len to copy");
        } else {
            size_t off = 0;
            size_t idx = 0;
            while (len > 0) {
                buffernode& node = bl[idx];
                memcpy(buf+off, node.buf, max(len, node.len));
                len -= max(len, node.len);
                off += max(len, node.len);
                idx++;
            }
        }
    }

    void copy(char *buf, size_t len, size_t offset) {
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
                buffernode& node = bl[idx];
                memcpy(buf+off, node.buf+offset, max(length, node.len-offset));
                length -= max(length, node.len);
                off += max(length, node.len);
                idx++;
                offset = 0;
            }
        }
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

    void rebuild_aligned_size_and_memory(unsigned align_size) {
        buffernode node;
        node.len = capacity;
        node.is_align;
        uint32_t align_len = align_up(capacity);
        node.buf = aligned_malloc(align_len, align_size);
        copy(node.buf, capacity);
        clear_free();
        bl.push_back(node);
    }

private:
    bufferlist_v bl;
    uint32_t capacity;
};

#endif //BUFFERLIST_H