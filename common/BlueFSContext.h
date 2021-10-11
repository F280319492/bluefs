#ifndef BLUEFSCONTEXT_H
#define BLUEFSCONTEXT_H

#include <assert.h>
#include <atomic>
#include <iostream>
#include <fstream>
#include <string>
#include <boost/program_options.hpp>
namespace  bpo = boost::program_options;

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
        ParseConf("/home/conf.conf");
    }
    ~BlueFSContext() {
        if (_conf) {
            delete _conf;
            _conf = nullptr;
        }
    }

    void ParseConf(const std::string conf_path) {
        bpo::options_description fileOptions{"File"};
        bpo::variables_map vm;

        fileOptions.add_options()
                ("bdev_aio_max_queue_depth",        bpo::value<uint32_t>()->default_value(1024), "")
                ("bdev_aio",                        bpo::value<bool>()->default_value(true), "")
                ("bdev_block_size",                 bpo::value<uint64_t>()->default_value(4096), "")
                ("bdev_aio_reap_max",               bpo::value<uint32_t>()->default_value(30), "")
                ("bdev_aio_poll_ms",                bpo::value<uint32_t>()->default_value(5), "")
                ("bluefs_max_log_runway",           bpo::value<uint64_t>()->default_value(4194304), "")
                ("bluefs_allocator",                bpo::value<std::string>()->default_value("stable"), "")
                ("bluefs_max_prefetch",             bpo::value<uint64_t>()->default_value(1048576), "")
                ("bluefs_buffered_io",              bpo::value<bool>()->default_value(false), "")
                ("bluefs_compact_log_sync",         bpo::value<bool>()->default_value(false), "")
                ("bluefs_log_compact_min_size",     bpo::value<uint64_t>()->default_value(16777216), "")
                ("bluefs_log_compact_min_ratio",    bpo::value<float>()->default_value(5.0), "")
                ("bluefs_min_log_runway",           bpo::value<uint64_t>()->default_value(1048576), "")
                ("bluefs_preextend_wal_files",      bpo::value<bool>()->default_value(false), "")
                ("bluefs_sync_write",               bpo::value<bool>()->default_value(false), "")
                ("bluefs_min_flush_size",           bpo::value<uint64_t>()->default_value(524288), "")
                ("bluefs_alloc_size",               bpo::value<uint64_t>()->default_value(16777216), "");

        try {
            std::ifstream ifs(conf_path);
            if (ifs) {
                store(parse_config_file(ifs, fileOptions), vm);
            }
            notify(vm);
        }
        catch(...) {
            std::cout << "unkown config!\n";
            return;
        }

        if (vm.count("bdev_aio_max_queue_depth")) {
            _conf->bdev_aio_max_queue_depth = vm["bdev_aio_max_queue_depth"].as<uint32_t>();
        }
        if (vm.count("bdev_aio")) {
            _conf->bdev_aio = vm["bdev_aio"].as<bool>();
        }
        if (vm.count("bdev_block_size")) {
            _conf->bdev_block_size = vm["bdev_block_size"].as<uint64_t>();
        }
        if (vm.count("bdev_aio_reap_max")) {
            _conf->bdev_aio_reap_max = vm["bdev_aio_reap_max"].as<uint32_t>();
        }
        if (vm.count("bdev_aio_poll_ms")) {
            _conf->bdev_aio_poll_ms = vm["bdev_aio_poll_ms"].as<uint32_t>();
        }
        if (vm.count("bluefs_max_log_runway")) {
            _conf->bluefs_max_log_runway = vm["bluefs_max_log_runway"].as<uint64_t>();
        }
        if (vm.count("bluefs_allocator")) {
            _conf->bluefs_allocator = vm["bluefs_allocator"].as<std::string>();
        }
        if (vm.count("bluefs_max_prefetch")) {
            _conf->bluefs_max_prefetch = vm["bluefs_max_prefetch"].as<uint64_t>();
        }
        if (vm.count("bluefs_buffered_io")) {
            _conf->bluefs_buffered_io = vm["bluefs_buffered_io"].as<bool>();
        }
        if (vm.count("bluefs_compact_log_sync")) {
            _conf->bluefs_compact_log_sync = vm["bluefs_compact_log_sync"].as<bool>();
        }
        if (vm.count("bluefs_log_compact_min_size")) {
            _conf->bluefs_log_compact_min_size = vm["bluefs_log_compact_min_size"].as<uint64_t>();
        }
        if (vm.count("bluefs_log_compact_min_ratio")) {
            _conf->bluefs_log_compact_min_ratio = vm["bluefs_log_compact_min_ratio"].as<float>();
        }
        if (vm.count("bluefs_min_log_runway")) {
            _conf->bluefs_min_log_runway = vm["bluefs_min_log_runway"].as<uint64_t>();
        }
        if (vm.count("bluefs_preextend_wal_files")) {
            _conf->bluefs_preextend_wal_files = vm["bluefs_preextend_wal_files"].as<bool>();
        }
        if (vm.count("bluefs_sync_write")) {
            _conf->bluefs_sync_write = vm["bluefs_sync_write"].as<bool>();
        }
        if (vm.count("bluefs_min_flush_size")) {
            _conf->bluefs_min_flush_size = vm["bluefs_min_flush_size"].as<uint64_t>();
        }
        if (vm.count("bluefs_alloc_size")) {
            _conf->bluefs_alloc_size = vm["bluefs_alloc_size"].as<uint64_t>();
        }
        if (vm.empty()) {
            std::cout << "no options found \n";
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
            (void) snprintf(buffer, sizeof(buffer), "%" PRId64 "%s", (unsigned long long)n, u);
        } else if ((v % mult) == 0) {
            // If this is an even multiple of the base, always display
            // without any decimal fraction.
            (void) snprintf(buffer, sizeof(buffer), "%" PRId64 "%s", (unsigned long long)n, u);
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