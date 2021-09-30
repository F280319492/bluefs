
#include "StableAllocator.h"

StableAllocator::StableAllocator(BlueFSContext* cct, uint32_t size, const std::string& name)
    : Allocator(name), cct(cct), stable_size(size), num_free(0), last_alloc(0)
{
}

StableAllocator::~StableAllocator()
{
}

int64_t StableAllocator::allocate(
        uint64_t want_size, uint64_t alloc_unit, uint64_t max_alloc_size,
        int64_t hint, PExtentVector *extents) {
    dout(10) << __func__ << " want_size 0x" << std::hex << want_size
	   << " alloc_unit 0x" << alloc_unit
	   << " hint 0x" << hint << std::dec
	   << dendl;

    (void) max_alloc_size;
    uint64_t allocated_size = 0;
    uint64_t offset = 0;
    uint32_t length = stable_size;
    std::lock_guard<std::mutex> l(lock);
    while (allocated_size < want_size) {
        if (free_list.empty()) { // no space
            break;
        }
        if (!hint) {
            hint = last_alloc;
        }

        auto p = free_list.lower_bound(hint);
        if (p == free_list.end()) {
            offset = *p;
        } else {
            offset = *free_list.begin();
        }
        
        extents->push_back(bluefs_pextent_t(offset, length));
        free_list.erase(offset);

        allocated_size += length;
        hint = offset + length;
    }
    
    if (allocated_size == 0) {
        return -ENOSPC;
    }
    return allocated_size;
}

void StableAllocator::release(const PExtentVector& release_set) {
    std::lock_guard<std::mutex> l(lock);
    for (auto& p_extent : release_set) {
        assert(p_extent.length == stable_size);
        if (free_list.find(p_extent.offset) == free_list.end()) {
            free_list.insert(p_extent.offset);
            num_free += p_extent.length;
        } else {
            dout(1) << __func__ << " offset " << p_extent.offset << " has in free list." << dendl;
        }
    }
}

uint64_t StableAllocator::get_free() {
    std::lock_guard<std::mutex> l(lock);
    return num_free;
}

double StableAllocator::get_fragmentation(uint64_t alloc_unit) {
    return 0.0;
}

void StableAllocator::dump() {
    std::lock_guard<std::mutex> l(lock);
    for (auto off : free_list) {
        dout(0) << __func__ << "  0x" << std::hex << off << "~"
	      << stable_size << std::dec << dendl;
    }
}

void StableAllocator::dump(std::function<void(uint64_t offset, uint64_t length)> notify) {
    std::lock_guard<std::mutex> l(lock);
    for (auto off : free_list) {
        notify(off, stable_size);
    }
}

void StableAllocator::init_add_free(uint64_t offset, uint64_t length) {
    dout(10) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << std::dec << dendl;
    assert(length == stable_size);
    std::lock_guard<std::mutex> l(lock);
    if (free_list.find(offset) == free_list.end()) {
        free_list.insert(offset);
        num_free += length;
    } else {
        dout(1) << __func__ << " offset " << offset << " has in free list." << dendl;
    }
}

void StableAllocator::init_rm_free(uint64_t offset, uint64_t length) {
    dout(10) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << std::dec << dendl;
    assert(length == stable_size);
    std::lock_guard<std::mutex> l(lock);
    if (free_list.find(offset) != free_list.end()) {
        free_list.erase(offset);
        num_free -= length;
    } else {
        dout(1) << __func__ << " offset " << offset << " does not in free list." << dendl;
    }
}

void StableAllocator::shutdown() {
    dout(1) << __func__ << dendl;
}