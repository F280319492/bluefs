#include "utime.h"

utime_t clock_now() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    utime_t n(tv);
    return n;
}