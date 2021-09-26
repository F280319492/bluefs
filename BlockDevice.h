
#ifndef BLOCKDEVICE_H
#define BLOCKDEVICE_H

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <list>
#include <map>
#include "bufferlist.h"
#include "BlueFSContext.h"

#define SPDK_PREFIX "spdk:"

struct IOContext {

};

class BlockDevice {
public:
    BlueFSContext* cct;

public:
    BlockDevice(BlueFSContext* cct) : cct(cct) {}
    virtual ~BlockDevice() = default;
    typedef void (*aio_callback_t)(void *handle, void *aio);

    static BlockDevice *create( const std::string& path, aio_callback_t cb, void *cbpriv);

    virtual void aio_submit(IOContext *ioc) = 0;

    virtual uint64_t get_size() const = 0;
    virtual uint64_t get_block_size() const = 0;

    virtual int read(
        uint64_t off,
        uint64_t len,
        bufferlist *pbl,
        IOContext *ioc,
        bool buffered) = 0;
      
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

#endif //BLOCKDEVICE_H
