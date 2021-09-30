#ifndef ALLOCATOR_H
#define ALLOCATOR_H

#include <stdint.h>
#include <iostream>
#include <vector>
#include <string>
#include <functional>

#include "common/BlueFSContext.h"
#include "common/debug.h"

// pextent: physical extent
struct bluefs_pextent_t
{
    static const uint64_t INVALID_OFFSET = ~0ull;

    uint64_t offset = 0;
    uint64_t length = 0;

    bluefs_pextent_t(){}
    bluefs_pextent_t(uint64_t o, uint64_t l) : offset(o), length(l) {}

    bool is_valid() const {
        return offset != INVALID_OFFSET;
    }
    uint64_t end() const {
        return offset != INVALID_OFFSET ? offset + length : INVALID_OFFSET;
    }

    bool operator==(const bluefs_pextent_t& other) const {
        return offset == other.offset && length == other.length;
    }
};

std::ostream& operator<<(std::ostream& out, const bluefs_pextent_t& o);

typedef std::vector<bluefs_pextent_t> PExtentVector;


class Allocator {
public:
  explicit Allocator(const std::string& name) {}
  virtual ~Allocator() {}

  /*
   * Allocate required number of blocks in n number of extents.
   * Min and Max number of extents are limited by:
   * a. alloc unit
   * b. max_alloc_size.
   * as no extent can be lesser than alloc_unit and greater than max_alloc size.
   * Apart from that extents can vary between these lower and higher limits according
   * to free block search algorithm and availability of contiguous space.
   */
  virtual int64_t allocate(uint64_t want_size, uint64_t alloc_unit,
			   uint64_t max_alloc_size, int64_t hint,
			   PExtentVector *extents) = 0;

  int64_t allocate(uint64_t want_size, uint64_t alloc_unit,
		   int64_t hint, PExtentVector *extents) {
    return allocate(want_size, alloc_unit, want_size, hint, extents);
  }

  /* Bulk release. Implementations may override this method to handle the whole
   * set at once. This could save e.g. unnecessary mutex dance. */
  virtual void release(const PExtentVector& release_set) = 0;

  virtual void dump() = 0;
  virtual void dump(std::function<void(uint64_t offset, uint64_t length)> notify) = 0;

  virtual void init_add_free(uint64_t offset, uint64_t length) = 0;
  virtual void init_rm_free(uint64_t offset, uint64_t length) = 0;

  virtual uint64_t get_free() = 0;
  virtual double get_fragmentation(uint64_t alloc_unit)
  {
    return 0.0;
  }
  virtual double get_fragmentation_score() {
    return 0.0;
  }
  virtual void shutdown() = 0;

  static Allocator *create(BlueFSContext* cct, std::string type, int64_t size,
			   int64_t block_size, const std::string& name = "");
};

#endif //ALLOCATOR_H