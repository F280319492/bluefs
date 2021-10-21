#ifndef DEBUG_H
#define DEBUG_H

#include <iostream>
#include <sstream>
#include <string.h>

#define INFOLEVEL 1
#define DEBUGLEVEL 2
#define WARNINGLEVEL 3
#define ERRORLEVEL 4

#define dout(level)  (level - DEBUGLEVEL <=0 ) && std::cout << __FILE__ << ":" << __LINE__ << ":" << __func__ << " "
#define rdout(level) (level - INFOLEVEL >=0 ) && std::cout
#define dendl std::endl
#define derr std::cerr << __FILE__ << ":" << __LINE__ << ":" << __func__ << " "

#define ddout std::cout << __FILE__ << ":" << __LINE__ << ":" << __func__ << " "

std::string cpp_strerror(int err);
void PrintBuffer(const void* pBuff, unsigned int nLen);

#endif //DEBUG_H