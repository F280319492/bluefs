#include "Allocator.h"

std::ostream& operator<<(std::ostream& out, const bluefs_pextent_t& o) {
    if (o.is_valid())
        return out << "0x" << std::hex << o.offset << "~" << o.length << std::dec;
    else
        return out << "!~" << std::hex << o.length << std::dec;
}