#ifndef UTIME_H
#define UTIME_H

#pragma once

#include <sys/time.h>
#include <iostream>

#include "bufferlist.h"

struct utime_t {
    u_int32_t tv_sec;
    u_int32_t tv_usec;
    utime_t() : tv_sec(0), tv_usec(0) {}
    utime_t(struct timeval& t) : tv_sec(t.tv_sec), tv_usec(t.tv_usec) {}
    utime_t(u_int32_t s, u_int32_t us) : tv_sec(s), tv_usec(us) {}

    utime_t operator+(const utime_t& other_t) {
        return utime_t(tv_sec + other_t.tv_sec + (tv_usec+other_t.tv_usec)/1000000L,
                    (tv_usec+other_t.tv_usec) % 1000000L);
    }

    utime_t operator-(const utime_t& other_t) {
        return utime_t(tv_sec - other_t.tv_sec - (tv_usec<other_t.tv_usec ? 1 : 0),
                    tv_usec - other_t.tv_usec + (tv_usec<other_t.tv_usec ? 1000000 : 0));
    }

    void encode(bufferlist& bl) const {
        bl.encode_num(&tv_sec, sizeof(tv_sec));
        bl.encode_num(&tv_usec, sizeof(tv_usec));
    }

    void decode(bufferlist& bl) {
        bl.decode_num(&tv_sec, sizeof(tv_sec));
        bl.decode_num(&tv_usec, sizeof(tv_usec));
    }

    uint64_t sec() {
        return tv_sec;
    }

    uint64_t usec() {
        return tv_usec;
    }

};

// ostream
inline std::ostream& operator<<(std::ostream& out, const utime_t& t)
{
    out << (t.tv_sec + t.tv_usec/1000000.0) << "s";
    return out;
}

utime_t clock_now();

#endif //UTIME_H