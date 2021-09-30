#ifndef BLUEFSCONTEXT_H
#define BLUEFSCONTEXT_H

#include <assert.h>
#include <atomic>
#include <iostream>

#define PRId64  "llu"

struct bulefs_config {
    uint32_t bdev_aio_max_queue_depth;
    bool bdev_aio;
    uint64_t bdev_block_size;
    uint32_t bdev_aio_reap_max;
    uint32_t bdev_aio_poll_ms;
    uint64_t bluefs_max_log_runway; //4194304
    std::string bluefs_allocator; //stable
    uint64_t bluefs_max_prefetch; //1048576
    bool bluefs_buffered_io;
    bool bluefs_compact_log_sync; //false
    uint64_t bluefs_log_compact_min_size; //16777216
    float bluefs_log_compact_min_ratio; //5.000000
    uint64_t bluefs_min_log_runway; //1048576
    bool bluefs_preextend_wal_files; //false
    bool bluefs_sync_write; //false
    uint64_t bluefs_min_flush_size; //524288
    uint64_t bluefs_alloc_size;
};

class BlueFSContext {
public:
    BlueFSContext(const BlueFSContext &) = delete;
    BlueFSContext &operator =(const BlueFSContext &) = delete;
    BlueFSContext(BlueFSContext &&) = delete;
    BlueFSContext &operator =(BlueFSContext &&) = delete;
    BlueFSContext() {
      _conf = new bulefs_config;
    }
    ~BlueFSContext() {
      if (_conf) {
        delete _conf;
        _conf = nullptr;
      }
    }
private:
    std::atomic<unsigned> nref;
public:
    BlueFSContext *get() {
        ++nref;
        return this;
    }

    void put() {
        assert(nref > 0);
        if (--nref == 0) {
            delete this;
        }
    }
    bulefs_config* _conf;
};

namespace {
  inline std::ostream& format_u(std::ostream& out, const uint64_t v, const uint64_t n,
      const int index, const uint64_t mult, const char* u)
  {
    char buffer[32];

    if (index == 0) {
      (void) snprintf(buffer, sizeof(buffer), "%" PRId64 "%s", n, u);
    } else if ((v % mult) == 0) {
      // If this is an even multiple of the base, always display
      // without any decimal fraction.
      (void) snprintf(buffer, sizeof(buffer), "%" PRId64 "%s", n, u);
    } else {
      // We want to choose a precision that reflects the best choice
      // for fitting in 5 characters.  This can get rather tricky when
      // we have numbers that are very close to an order of magnitude.
      // For example, when displaying 10239 (which is really 9.999K),
      // we want only a single place of precision for 10.0K.  We could
      // develop some complex heuristics for this, but it's much
      // easier just to try each combination in turn.
      int i;
      for (i = 2; i >= 0; i--) {
        if (snprintf(buffer, sizeof(buffer), "%.*f%s", i,
          static_cast<double>(v) / mult, u) <= 7)
          break;
      }
    }

    return out << buffer;
  }
}

struct byte_u_t {
  uint64_t v;
  explicit byte_u_t(uint64_t _v) : v(_v) {};
};

inline std::ostream& operator<<(std::ostream& out, const byte_u_t& b)
{
  uint64_t n = b.v;
  int index = 0;
  const char* u[] = {"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"};

  while (n >= 1024 && index < 7) {
    n /= 1024;
    index++;
  }

  return format_u(out, b.v, n, index, 1ULL << (10 * index), u[index]);
}

#endif //BLUEFSCONTEXT_H