#ifndef UTIME_H
#define UTIME_H

#include <sys/time.h>
#include <iostream>

struct utime_t {
    u_int32_t tv_sec;
    u_int32_t tv_usec;
    utime_t() : tv_sec(0), tv_usec(0) {}
    utime_t(struct timeval& t) : tv_sec(t.tv_sec), tv_usec(t.tv_usec) {}
    utime_t(u_int32_t sec, u_int32_t usec) : tv_sec(sec), tv_usec(usec) {}

    utime_t operator+(const utime_t& other_t) {
        return utime_t(tv_sec + other_t.tv_sec + (tv_usec+other_t.tv_usec)/1000000L,
                    (tv_usec+other_t.tv_usec) % 1000000L);
    }

    utime_t operator-(const utime_t& other_t) {
        return utime_t(tv_sec - other_t.tv_sec - (tv_usec<other_t.tv_usec ? 1 : 0),
                    tv_usec - other_t.tv_usec + (tv_usec<other_t.tv_usec ? 1000000 : 0));
    }
};

// ostream
inline std::ostream& operator<<(std::ostream& out, const utime_t& t)
{
    out << (t.tv_sec + t.tv_usec/1000000.0) << "s" << std::endl;
    return out;
}

utime_t clock_now() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    utime_t n(tv);
    return n;
}

#endif //UTIME_H