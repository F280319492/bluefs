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

void PrintBuffer(const void* pBuff, unsigned int nLen)
{
    if (NULL == pBuff || 0 == nLen) {
        return;
    }

    const int nBytePerLine = 16;
    unsigned char* p = (unsigned char*)pBuff;
    char szHex[3*nBytePerLine+1] = {0};

    std::cout <<  "-----------------begin-------------------" << std::endl;
    for (unsigned int i=0; i<nLen; ++i) {
        int idx = 3 * (i % nBytePerLine);
        if (0 == idx) {
            memset(szHex, 0, sizeof(szHex));
        }
        snprintf(&szHex[idx], 4, "%02x ", p[i]); // buff长度要多传入1个字节

        // 以16个字节为一行，进行打印
        if (0 == ((i+1) % nBytePerLine)) {
            std::cout << szHex << std::endl;
        }
    }

    // 打印最后一行未满16个字节的内容
    if (0 != (nLen % nBytePerLine)) {
        std::cout << szHex << std::endl;
    }
    std::cout << "------------------end-------------------\n" << std::endl;
}
