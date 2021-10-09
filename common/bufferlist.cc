#include <stdlib.h>
#include <algorithm>
#include "bufferlist.h"

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

bool bufferlist::encode(const void* buf, size_t len) {
    if (bl.empty() || bl.back().len+len > bl.back().cap) {
        size_t alloc_size = std::max(len, (size_t)ALLOC_SIZE);
        alloc_size = align_up(alloc_size, (size_t)ALLOC_SIZE);
        void* buf = aligned_malloc(alloc_size, ALLOC_SIZE);
        if (!buf) {
            derr << __func__ << " aligned_malloc failed!" << dendl;
            return false;
        }
        bl.push_back(buffernode(buf, 0, alloc_size, true, true));
    }
    buffernode& node = bl.back();

    memcpy((char*)node.buf+node.len, buf, len);
    node.len += len;

    capacity += len;
}

template <typename T>
bool bufferlist::encode_num(const T v) {
    return encode(&v, sizeof(v));
}

bool bufferlist::encode_str(const std::string& str) {
    uint32_t len = str.length();
    encode_num(len);
    if (len) {
        encode(str.data(), len);
    }
}

bool bufferlist::encode_bufferlist(const bufferlist& bl) {
    uint32_t len = bl.length();
    encode_num(len);
    if (len) {
        void* buf = malloc(len);
        bl.copy(buf, len);
        encode_num(len);
        free(buf);
    }
}

bool bufferlist::decode(void* buf, size_t len) {
    size_t copy_len = len;
    size_t copy_off = 0;
    while (len > 0) {
        memcpy((char*)buf+off, (char*)bl[idx].buf+off, std::min(len, (size_t)bl[idx].len-off));

        copy_off += std::min(len, (size_t)bl[idx].len-off);
        off += std::min(len, (size_t)bl[idx].len-off);
        len -= std::min(len, (size_t)bl[idx].len-off);

        if (off == bl[idx].len) {
            idx++;
            off = 0;
        }
    }
    return true; 
}

template <typename T>
bool bufferlist::decode_num(T* v) {
    return decode(v, sizeof(*v));
}


bool bufferlist::decode_str(std::string* str) {
    uint32_t len;
    decode_num(&len);
    if (len) {
        void* buf = malloc(len);
        copy(buf, len);
        str->append((char*)buf, len);
        free(buf);
    }
}

bool bufferlist::decode_bufferlist(bufferlist* bl) {
    uint32_t len;
    void* buf;
    decode_num(&len);

    size_t alloc_size = align_up(len, (uint32_t)ALLOC_SIZE);
    buf = aligned_malloc(alloc_size, ALLOC_SIZE);
    decode(buf, len);

    bl->append((char*)buf, (size_t)len, alloc_size, true, true);
}