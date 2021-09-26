#ifndef BUFFERLIST_H
#define BUFFERLIST_H

#include <list>
#include <vector>
#include <iostream>
#include <stdlib.h>

using bufferlist_v = std::vector<uint8_t>;

inline void uint64insert(bufferlist_v &bl, uint64_t x) {

    uint8_t *uint8_ptr = (uint8_t*)&x;

    for (int i = 0; i < 8; i++) {
        bl.emplace_back(*(uint8_ptr + i));
    }
}

inline void uint64insert(std::list<unsigned char> &bl, uint64_t x) {

    uint8_t *uint8_ptr = (uint8_t*)&x;

    for (int i = 0; i < 8; i++) {
        bl.emplace_back(*(uint8_ptr + i));
    }
}

inline void append(bufferlist_v &bl, const char *buf, size_t len) {
    bl.insert(bl.end(), buf, buf + len);
}

inline void copy(const bufferlist_v &bl, bufferlist_v::const_iterator &p, size_t len, char *buf) {

    long length = len;

    while (length > 0) {
        if (bl.end() == p) {
            throw std::range_error("error bufferlist_v len to copy");
        }

        *buf = *(p++);
        length--;
    }

}

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

class bufferlist {
public:
    void iter_start() {
        dec_pos = bl.begin();
    }

    void clear() {
        bl.clear();
        dec_pos = bl.end();
    }

    void append(const char *buf, size_t len) {
        bl.insert(bl.end(), buf, buf + len);
    }

    template<typename T>
    void append_var(T v)
    {
        char *ptr = (char*)&v;
        bl.insert(bl.end(), ptr, ptr + sizeof(v));
    }

    template<typename T>
    void append_struct(const T &v) {
        v.encode(*this);
    }

    template<typename T>
    void append_vector(const std::vector<T> &v, bool varint = false) {
        uint32_t num = v.size();
        append_var(num);
        if (varint) {
            for (auto p : v) {
                append_var(p);
            }
        }
        else {
            for (auto p : v) {
                p.encode(*this);
            }
        }
    }

    void append_string(const std::string_view &s) {
        uint32_t num = s.size();
        append_var(num);
        bl.insert(bl.end(), s.begin(), s.end());
    }

    void append_bufferlist(const bufferlist &src_bl) {
        uint32_t num = src_bl.size();
        append_var(num);
        
        //bl.insert(bl.end(), src_bl.const_buf().begin(), src_bl.const_buf().end()); 出现浅拷贝问题
        for (auto p : src_bl.const_buf()) {
            bl.push_back(p);
        }
    }

    bufferlist_v::const_iterator copy(size_t len, char *buf) {
        long length = len;
        while (length > 0) {
            if (dec_pos == bl.end()) {
                throw std::range_error("error bufferlist_v len to copy");
            }

            *buf = *(dec_pos++);
            length--;
        }

        return dec_pos;
    }

    template<typename T>
    bufferlist_v::const_iterator copy_to_var(T &v) {
        char *ptr = (char*)&v;
        long length = sizeof(v);
        while (length > 0) {
            if (dec_pos == bl.end()) {
                throw std::range_error("error bufferlist_v len to copy");
            }

            *(ptr++) = *(dec_pos++);
            length--;
        }

        return dec_pos;
    }

    template<typename T>
    bufferlist_v::const_iterator copy_to_vector(std::vector<T> &v, bool varint = false) {
        uint32_t num = 0;
        copy_to_var(num);
        if (varint) {
            for (int i = 0; i < num; i++) {
                T t;
                copy_to_var(t);
                v.push_back(t);
            }
        }
        else {
            for (int i = 0; i < num; i++) {
                T t;
                t.decode(*this);
                v.push_back(t);
            }
        }

        return dec_pos;
    }

    bufferlist_v::const_iterator copy_to_string(std::vector<char> &dst, char *s = nullptr) {
        uint32_t num = 0;
        copy_to_var(num);
        for (int i = 0; i < num; i++) {
            char c;
            copy_to_var(c);
            dst.push_back(c);
        }

        if (s != nullptr) {
            char *ptr = s;
            for (auto c : dst) {
                *(ptr++) = c;
            }
        }

        return dec_pos;
    }
    bufferlist_v::const_iterator copy_to_bufferlist(bufferlist &dst, char *s = nullptr) {
        uint32_t num = 0;
        copy_to_var(num);
        for (int i = 0; i < num; i++) {
            char c;
            copy_to_var(c);
            dst.push_back(c);
        }

        if (s != nullptr) {
            char *ptr = s;
            for (auto c : dst.const_buf()) {
                *(ptr++) = c;
            }
        }

        return dec_pos;
    }

    std::size_t size() const{
        return bl.size();
    }

    void push_back(char c) {
        bl.push_back(c);
    }

    bufferlist_v const_buf() const{
        return bl;
    }


private:
    bufferlist_v bl;
    bufferlist_v::const_iterator dec_pos;
};

template <typename T>
inline constexpr T align_up(T v, T align) {
  return (v + align - 1) & ~(align - 1);
}

template <typename T>
inline constexpr T align_down(T v, T align) {
  return v & ~(align - 1);
}

#endif //BUFFERLIST_H