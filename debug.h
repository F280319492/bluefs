#include <iostream>

#define INFOLEVEL 1
#define DEBUGLEVEL 2
#define WARNINGLEVEL 3
#define ERRORLEVEL 4

#define dout(level) (level - DEBUGLEVEL >=0 ) && std::cout 
#define dendl std::endl
#define derr std::cerr
