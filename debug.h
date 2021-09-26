#ifndef DEBUG_H
#define DEBUG_H

#include <iostream>
#include <sstream>
#include <string.h>

#define INFOLEVEL 1
#define DEBUGLEVEL 2
#define WARNINGLEVEL 3
#define ERRORLEVEL 4

#define dout(level) (level - DEBUGLEVEL >=0 ) && std::cout 
#define dendl std::endl
#define derr std::cerr


std::string cpp_strerror(int err) {
    char buf[128];
    char *errmsg;

    if (err < 0)
        err = -err;
    std::ostringstream oss;
    buf[0] = '\0';

    // strerror_r returns char * on Linux, and does not always fill buf
    strerror_r(err, buf, sizeof(buf));
    errmsg = buf;

    oss << "(" << err << ") " << errmsg;

    return oss.str();
}

#endif //DEBUG_H