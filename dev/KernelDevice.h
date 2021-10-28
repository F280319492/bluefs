#ifndef KERNELDEVICE_H
#define KERNELDEVICE_H

#include <atomic>

#include "common/aio.h"
#include "BlockDevice.h"
#include "common/BlueFSContext.h"
#include <thread>
#include <pthread.h>
#include <vector>

#ifndef RW_IO_MAX
#define RW_IO_MAX 0x7FFFF000
#endif

#ifndef MAX_DEV_THREAD
#define MAX_DEV_THREAD 5
#endif

class KernelDevice : public BlockDevice {

    uint64_t size;
    uint64_t block_size;
    std::string path;
    bool aio, dio;

    std::atomic<bool> io_since_flush = {false};
    std::mutex flush_mutex;

    int thread_num;
    std::vector<int> fd_directs;
    std::vector<int> fd_buffereds;
    std::vector<aio_queue_t> aio_queues;
    aio_callback_t aio_callback;
    void *aio_callback_priv;
    std::vector<bool> aio_stops;
    std::thread aio_threads[MAX_DEV_THREAD];

    void _aio_thread(int idx);
    int _aio_start();
    void _aio_stop();

    int _sync_write(uint64_t off, bufferlist& bl, bool buffered);

    int _lock();

    int direct_read_unaligned(uint64_t off, uint64_t len, char *buf);

public:
    KernelDevice(BlueFSContext* cct, aio_callback_t cb, void *cbpriv);

    void aio_submit(IOContext *ioc, bool fixed_thread = true) override;

    uint64_t get_size() const override {
        return size;
    }

    uint64_t get_block_size() const override {
        return block_size;
    }

    int read(uint64_t off, uint64_t len, bufferlist *pbl,
        IOContext *ioc,
        bool buffered) override;
    int aio_read(uint64_t off, uint64_t len, bufferlist *pbl,
            IOContext *ioc) override;
    int aio_read(uint64_t off, uint64_t len, char *buf,
                 bufferlist *pbl, IOContext *ioc) override;
    int read_random(uint64_t off, uint64_t len, char *buf, bool buffered) override;

    int write(uint64_t off, bufferlist& bl, bool buffered) override;
    int aio_write(uint64_t off, bufferlist& bl,
            IOContext *ioc,
            bool buffered) override;
    int flush() override;

    // for managing buffered readers/writers
    int invalidate_cache(uint64_t off, uint64_t len) override;
    int open(const std::string& path) override;
    void close() override;
};

#endif //KERNELDEVICE_H
