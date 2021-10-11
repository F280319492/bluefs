
#include "StableAllocator.h"
#include "common/bufferlist.h"

StableAllocator::StableAllocator(BlueFSContext* cct, uint32_t alloc_size, const std::string& name)
    : Allocator(name), cct(cct), stable_size(alloc_size), num_free(0), last_alloc(0)
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
    uint64_t offset;
    uint64_t length;
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
            p = free_list.begin();
        }
        offset = p.get_start();
        length = align_up(want_size-allocated_size, (uint64_t)stable_size);
        length = std::min(length, p.get_len());
        extents->push_back(bluefs_pextent_t(offset, length));
        free_list.erase(offset, length);

        allocated_size += length;
        hint = offset + length;
        // TODO
        num_free -= length;
    }
    
    if (allocated_size == 0) {
        return -ENOSPC;
    }
    return allocated_size;
}

void StableAllocator::release(const PExtentVector& release_set) {
    std::lock_guard<std::mutex> l(lock);
    for (auto& p_extent : release_set) {
        assert(!(p_extent.offset%stable_size));
        assert(!(p_extent.length%stable_size));
        free_list.insert(p_extent.offset, p_extent.length);
        //TODO
        num_free += p_extent.length;
    }
}

void StableAllocator::release(const interval_set<uint64_t>& release_set) {
    uint64_t off, len;
    std::lock_guard<std::mutex> l(lock);
    for (auto& p_extent : release_set) {
        off = p_extent.first;
        len = p_extent.second;
        assert(!(len % stable_size));
        assert(!(off % stable_size));
        free_list.insert(off, len);
        //TODO
        num_free += len;
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
    for (auto item : free_list) {
        dout(0) << __func__ << "  0x" << std::hex << item.first << "~"
	      << item.second << std::dec << dendl;
    }
}

void StableAllocator::dump(std::function<void(uint64_t offset, uint64_t length)> notify) {
    std::lock_guard<std::mutex> l(lock);
    for (auto item : free_list) {
        notify(item.first, item.second);
    }
}

void StableAllocator::init_add_free(uint64_t offset, uint64_t length) {
    dout(10) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << std::dec << dendl;
    uint64_t end_off = offset + length;
    offset = align_up(offset, (uint64_t)stable_size);
    length = align_down(end_off-offset, (uint64_t)stable_size);
    {
        std::lock_guard<std::mutex> l(lock);
        free_list.insert(offset, length);
        //TODO
        num_free += length;
    }
}

void StableAllocator::init_rm_free(uint64_t offset, uint64_t length) {
    dout(10) << __func__ << " 0x" << std::hex << offset << "~" << length
	   << std::dec << dendl;
    assert(!(offset % stable_size));
    assert(!(length % stable_size));
    {
        std::lock_guard<std::mutex> l(lock);
        free_list.erase(offset, length);
        //TODO
        num_free -= length;
    }
}

void StableAllocator::shutdown() {
    dout(1) << __func__ << dendl;
}