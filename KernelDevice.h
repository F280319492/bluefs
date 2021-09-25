#ifndef KERNELDEVICE_H
#define KERNELDEVICE_H

#include <atomic>

#include "aio.h"
#include "BlockDevice.h"

#ifndef RW_IO_MAX
#define RW_IO_MAX 0x7FFFF000
#endif


class KernelDevice : public BlockDevice {
    int fd_direct, fd_buffered;
    uint64_t size;
    uint64_t block_size;
    std::string path;
    bool aio, dio;

    std::mutex debug_lock;

    std::atomic<bool> io_since_flush = {false};
    std::mutex flush_mutex;

    aio_queue_t aio_queue;
    aio_callback_t aio_callback;
    void *aio_callback_priv;
    bool aio_stop;

    void _aio_thread();
    int _aio_start();
    void _aio_stop();

    int _sync_write(uint64_t off, bufferlist& bl, bool buffered);

    int _lock();

    int direct_read_unaligned(uint64_t off, uint64_t len, char *buf);

    public:
    KernelDevice(CephContext* cct, aio_callback_t cb, void *cbpriv);

    void aio_submit(IOContext *ioc) override;

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

#endif
