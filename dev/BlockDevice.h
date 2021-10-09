
#ifndef BLOCKDEVICE_H
#define BLOCKDEVICE_H

#include <errno.h>
#include <sys/ioctl.h>
#include <sys/ioctl.h>
#include <linux/fs.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <list>
#include <map>
#include "common/bufferlist.h"
#include "common/BlueFSContext.h"
#include "common/Context.h"
#include "common/aio.h"

#define SPDK_PREFIX "spdk:"

struct IOContext {
private:
    std::mutex lock;
    std::condition_variable cond;
    int r = 0;

public:
    BlueFSContext* cct;
    void *priv;
    //void *bak;
#ifdef HAVE_SPDK
    void *nvme_task_first = nullptr;
    void *nvme_task_last = nullptr;
#endif

    std::list<aio_t> pending_aios;    ///< not yet submitted
    std::list<aio_t> running_aios;    ///< submitting or submitted
    std::atomic_int num_pending = {0};
    std::atomic_int num_running = {0};
    bool allow_eio;
    Context *read_context;
    explicit IOContext(BlueFSContext* cct, void *p, bool allow_eio = false,
                        Context *read_context = nullptr)
        : cct(cct), priv(p), allow_eio(allow_eio), read_context(read_context) {}

    // no copying
    IOContext(const IOContext& other) = delete;
    IOContext &operator=(const IOContext& other) = delete;

    ~IOContext() {
        //mprotect(&read_context, 4*1024, PROT_READ|PROT_WRITE);
        read_context = nullptr;
    }

    bool has_pending_aios() {
        return num_pending.load();
    }

    void aio_wait();

    void try_aio_wake() {
        std::lock_guard<std::mutex> l(lock);
        if (num_running == 1) {
            // we might have some pending IOs submitted after the check
            // as there is no lock protection for aio_submit.
            // Hence we might have false conditional trigger.
            // aio_wait has to handle that hence do not care here.
            cond.notify_all();
            --num_running;
            assert(num_running >= 0);
        } else {
            --num_running;
        }
    }

    void set_return_value(int _r) {
        r = _r;
    }

    int get_return_value() const {
        return r;
    }
};

class BlockDevice {
public:
    BlueFSContext* cct;

public:
    BlockDevice(BlueFSContext* cct) : cct(cct) {}
    virtual ~BlockDevice() = default;
    typedef void (*aio_callback_t)(void *handle, void *aio);

    static BlockDevice *create(BlueFSContext* cct, const std::string& path, aio_callback_t cb, void *cbpriv);

    virtual void aio_submit(IOContext *ioc) = 0;

    virtual uint64_t get_size() const = 0;
    virtual uint64_t get_block_size() const = 0;

    virtual int read(
        uint64_t off,
        uint64_t len,
        bufferlist *pbl,
        IOContext *ioc,
        bool buffered) = 0;
    virtual int read_random(uint64_t off, uint64_t len, char *buf, bool buffered) = 0;
      
    virtual int write(
        uint64_t off,
        bufferlist& bl,
        bool buffered) = 0;

    virtual int aio_read(
        uint64_t off,
        uint64_t len,
        bufferlist *pbl,
        IOContext *ioc) = 0;

    virtual int aio_write(
        uint64_t off,
        bufferlist& bl,
        IOContext *ioc,
        bool buffered) = 0;

    virtual int flush() = 0;

    // for managing buffered readers/writers
    virtual int invalidate_cache(uint64_t off, uint64_t len) = 0;
    virtual int open(const std::string& path) = 0;
    virtual void close() = 0;
};

int get_block_device_size(int fd, int64_t *psize);

#endif //BLOCKDEVICE_H
