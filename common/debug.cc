#include <iostream>
#include <sstream>
#include <string.h>

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