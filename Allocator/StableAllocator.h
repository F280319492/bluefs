#ifndef STABLEALLOCATOR_H
#define STABLEALLOCATOR_H

#include <mutex>

#include "Allocator.h"

class StableAllocator : public Allocator {
private:
    BlueFSContext*                  cct;
    std::mutex                      lock;
    uint32_t                        stable_size;
    uint64_t                        num_free;   ///< total bytes in freelist
    interval_set<uint64_t>          free_list;

    uint64_t                        last_alloc;

public:
    StableAllocator(BlueFSContext* cct, uint32_t alloc_size, const std::string& name = "");
    ~StableAllocator() override;

    int64_t allocate(
        uint64_t want_size, uint64_t alloc_unit, uint64_t max_alloc_size,
        int64_t hint, PExtentVector *extents) override;
    
    void release(const PExtentVector& release_set) override;
    void release(const interval_set<uint64_t>& release_set) override;

    uint64_t get_free() override;
    double get_fragmentation(uint64_t alloc_unit) override;

    void dump() override;
    void dump(std::function<void(uint64_t offset, uint64_t length)> notify) override;

    void init_add_free(uint64_t offset, uint64_t length) override;
    void init_rm_free(uint64_t offset, uint64_t length) override;

    void shutdown() override;
};


#endif //STABLEALLOCATOR_H
