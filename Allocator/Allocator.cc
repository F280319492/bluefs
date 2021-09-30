#include "Allocator.h"
#include "StableAllocator.h"

std::ostream& operator<<(std::ostream& out, const bluefs_pextent_t& o) {
    if (o.is_valid())
        return out << "0x" << std::hex << o.offset << "~" << o.length << std::dec;
    else
        return out << "!~" << std::hex << o.length << std::dec;
}

Allocator *Allocator::create(BlueFSContext* cct, std::string type,
                             int64_t size, int64_t block_size, const std::string& name)
{
    Allocator* alloc = nullptr;
    if (type == "stable") {
        alloc = new StableAllocator(cct, size, name);
    }
    if (alloc == nullptr) {
        derr << "Allocator::" << __func__ << " unknown alloc type "
            << type << dendl;
    }
    return alloc;
}