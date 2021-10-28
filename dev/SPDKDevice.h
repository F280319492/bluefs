//
// Created by 费长红 on 2021/10/18.
//

#ifndef BLUEFS_SPDKDEVICE_H
#define BLUEFS_SPDKDEVICE_H

#include <queue>
#include <map>
#include <limits>
#include <thread>
#include <pthread.h>

#include "BlockDevice.h"
#include "common/interval_set.h"
#include "common/bufferlist.h"

enum class IOCommand {
    READ_COMMAND,
    WRITE_COMMAND,
    FLUSH_COMMAND
};

class SharedDriverData;
class SharedDriverQueueData;
struct Task;

class SPDKDevice : public BlockDevice {
    /**
   * points to pinned, physically contiguous memory region;
   * contains 4KB IDENTIFY structure for controller which is
   * target for CONTROLLER IDENTIFY command during initialization
   */
    SharedDriverData *driver;
    std::string name;
    SharedDriverQueueData *queue_t;
    std::deque<std::pair<Task *, IOContext *>> aio_queue;
    std::mutex aio_queue_lock;
    std::condition_variable aio_queue_cond;
    std::thread aio_thread;

    bool aio_stop;
    void _aio_thread();
    int _aio_start();
    void _aio_stop();

    uint64_t block_size;
    uint64_t size;

public:
    std::atomic_int queue_number = {0};
    aio_callback_t aio_callback;
    void *aio_callback_priv;

    SharedDriverData *get_driver() { return driver; }

    SPDKDevice(BlueFSContext* cct, aio_callback_t cb, void *cbpriv);

    uint64_t get_size() const override {
        return size;
    }
    uint64_t get_block_size() const override {
        return block_size;
    }

    void aio_submit(IOContext *ioc, bool fixed_thread = true) override;

    int read(uint64_t off, uint64_t len, bufferlist *pbl,
             IOContext *ioc,
             bool buffered) override;
    int aio_read(
            uint64_t off,
            uint64_t len,
            bufferlist *pbl,
            IOContext *ioc) override;
    int aio_read(
            uint64_t off,
            uint64_t len,
            char *buf,
            bufferlist *pbl,
            IOContext *ioc) override;
    int aio_write(uint64_t off, bufferlist& bl,
                  IOContext *ioc,
                  bool buffered) override;
    int write(uint64_t off, bufferlist& bl, bool buffered) override;
    int flush() override;
    int read_random(uint64_t off, uint64_t len, char *buf, bool buffered) override;

    // for managing buffered readers/writers
    int invalidate_cache(uint64_t off, uint64_t len) override;
    int open(const std::string& path) override;
    void close() override;

protected:
    bool is_valid_io(uint64_t off, uint64_t len) const {
        return (off % block_size == 0 &&
                len % block_size == 0 &&
                len > 0 &&
                off < size &&
                off + len <= size);
    }
};


#endif //BLUEFS_SPDKDEVICE_H
