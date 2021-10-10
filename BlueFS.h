#ifndef BLUEFS_H
#define BLUEFS_H

#include <map>
#include <set>
#include <unordered_map>
#include <atomic>

#include "bluefs_type.h"
#include "common/BlueFSContext.h"
#include "dev/BlockDevice.h"
#include "Allocator/Allocator.h"

#include "boost/intrusive/list.hpp"
#include "boost/intrusive_ptr.hpp"
#include "boost/algorithm/string/predicate.hpp"

class Allocator;

class BlueFS{
public:
    BlueFSContext* cct;
private:
    enum {
        WRITER_UNKNOWN,
        WRITER_WAL,
        WRITER_SST,
    };

    struct File {
        bluefs_fnode_t fnode;
        int refs;
        uint64_t dirty_seq;
        bool locked;
        bool deleted;
        boost::intrusive::list_member_hook<> dirty_item;

        std::atomic_int num_readers, num_writers;
        std::atomic_int num_reading;

        mutable std::atomic<uint64_t> nref;

        File() : refs(0),
            dirty_seq(0),
            locked(false),
            deleted(false),
            num_readers(0),
            num_writers(0),
            num_reading(0),
            nref(0) {}
        ~File() {
            assert(num_readers.load() == 0);
            assert(num_writers.load() == 0);
            assert(num_reading.load() == 0);
            assert(!locked);
        }

        const File *get() const {
            int v = ++nref;
            return this;
        }
        File *get() {
            int v = ++nref;
            return this;
        }
        void put() const {
            int v = --nref;
            if (v == 0) {
                delete this;
            }
        }

        uint64_t get_nref() const {
            return nref;
        }

        friend void intrusive_ptr_add_ref(File *f) {
            f->get();
        }
        friend void intrusive_ptr_release(File *f) {
            f->put();
        }
    };
    typedef boost::intrusive_ptr<File> FileRef;

    typedef boost::intrusive::list<File,
        boost::intrusive::member_hook<File,
	        boost::intrusive::list_member_hook<>,
	        &File::dirty_item> > dirty_file_list_t;

    struct Dir{
        std::map<std::string,FileRef> file_map;
        mutable std::atomic<uint64_t> nref;

        Dir() : nref(0) {}

        const Dir *get() const {
            int v = ++nref;
            return this;
        }
        Dir *get() {
            int v = ++nref;
            return this;
        }
        void put() const {
            int v = --nref;
            if (v == 0) {
                delete this;
            }
        }

        uint64_t get_nref() const {
            return nref;
        }

        friend void intrusive_ptr_add_ref(Dir *d) {
            d->get();
        }
        friend void intrusive_ptr_release(Dir *d) {
            d->put();
        }
    };
    typedef boost::intrusive_ptr<Dir> DirRef;

    struct FileWriter {
        FileRef file;
        uint64_t pos;           ///< start offset for buffer
        bufferlist buffer;      ///< new data to write (at end of file)
        int writer_type = 0;    ///< WRITER_*

        std::mutex lock;
        IOContext* iocv; ///< for each bdev

        FileWriter(FileRef f) : file(f), pos(0), {
            ++file->num_writers;
            iocv = nullptr;
        }
        // NOTE: caller must call BlueFS::close_writer()
        ~FileWriter() {
            --file->num_writers;
        }

        // note: BlueRocksEnv uses this append exclusively, so it's safe
        // to use buffer_appender exclusively here (e.g., it's notion of
        // offset will remain accurate).
        void append(const char *buf, size_t len) {
            buffer.encode(buf, len);
        }

        // note: used internally only, for ino 1 or 0.
        void append(bufferlist& bl) {
            buffer.append(bl);
        }

        uint64_t get_effective_write_pos() {
            return pos + buffer.length();
        }
    };

    struct FileReaderBuffer {
        uint64_t bl_off;        ///< prefetch buffer logical offset
        bufferlist bl;          ///< prefetch buffer
        uint64_t pos;           ///< current logical offset
        uint64_t max_prefetch;  ///< max allowed prefetch

        explicit FileReaderBuffer(uint64_t mpf) : bl_off(0),
                                                pos(0),
                                                max_prefetch(mpf) {}

        uint64_t get_buf_end() {
            return bl_off + bl.length();
        }
        uint64_t get_buf_remaining(uint64_t p) {
            if (p >= bl_off && p < bl_off + bl.length())
                return bl_off + bl.length() - p;
            return 0;
        }

        void skip(size_t n) {
            pos += n;
        }
        void seek(uint64_t offset) {
            pos = offset;
        }
    };

    struct FileReader {
        FileRef file;
        FileReaderBuffer buf;
        bool random;
        bool ignore_eof;        ///< used when reading our log file

        FileReader(FileRef f, uint64_t mpf, bool rand, bool ie) : file(f),
            buf(mpf),
            random(rand),
            ignore_eof(ie) {
            ++file->num_readers;
        }
        ~FileReader() {
            --file->num_readers;
        }
    };

    struct FileLock {
        FileRef file;
        explicit FileLock(FileRef f) : file(f) {}
    };

private:
    std::mutex lock;
    // cache
    std::map<std::string, DirRef> dir_map;              ///< dirname -> Dir
    std::unordered_map<uint64_t, FileRef> file_map; ///< ino -> File

    // map of dirty files, files of same dirty_seq are grouped into list.
    std::map<uint64_t, dirty_file_list_t> dirty_files;

    bluefs_super_t super;        ///< latest superblock (as last written)
    uint64_t ino_last = 0;       ///< last assigned ino (this one is in use)
    uint64_t log_seq = 0;        ///< last used log seq (by current pending log_t)
    uint64_t log_seq_stable = 0; ///< last stable/synced log seq
    FileWriter *log_writer = 0;  ///< writer for the log
    bluefs_transaction_t log_t;  ///< pending, unwritten log transaction
    bool log_flushing = false;   ///< true while flushing the log
    std::condition_variable log_cond;

    uint64_t new_log_jump_to = 0;
    uint64_t old_log_jump_to = 0;
    FileRef new_log = nullptr;
    FileWriter *new_log_writer = nullptr;

    BlockDevice*    bdev;                                       ///< block devices we can use
    IOContext*      ioc;                                        ///< IOContexts for bdevs
    interval_set<uint64_t> block_all;          ///< extents in bdev we own
    uint64_t        block_total;                                ///< sum of block_all
    Allocator*      alloc;                                      ///< allocators for bdevs
    uint64_t        alloc_size;                                 ///< alloc size for each device
    interval_set<uint64_t> pending_release;    ///< extents to release

    void _init_alloc();
    void _stop_alloc();

    void _pad_bl(bufferlist& bl);  ///< pad bufferlist to block size w/ zeros

    FileRef _get_file(uint64_t ino);
    void _drop_link(FileRef f);

    int _allocate(uint64_t len, bluefs_fnode_t* node);
    int _flush_all(FileWriter *h);
    int _flush(FileWriter *h, bool force);
    int _fsync(FileWriter *h, std::unique_lock<std::mutex>& l);

    void _claim_completed_aios(FileWriter *h, std::list<aio_t> *ls);
    void wait_for_aio(FileWriter *h);  // safe to call without a lock

    int _flush_and_sync_log(std::unique_lock<std::mutex>& l,
                uint64_t want_seq = 0,
                uint64_t jump_to = 0);
    uint64_t _estimate_log_size();
    bool _should_compact_log();
    void _compact_log_dump_metadata(bluefs_transaction_t *t);
    void _compact_log_sync();
    void _compact_log_async(std::unique_lock<std::mutex>& l);

    void _flush_bdev_safely(FileWriter *h);
    void flush_bdev();  // this is safe to call without a lock

    int _preallocate(FileRef f, uint64_t off, uint64_t len);

    int _read(
        FileReader *h,   ///< [in] read from here
        FileReaderBuffer *buf, ///< [in] reader state
        uint64_t offset, ///< [in] offset
        size_t len,      ///< [in] this many bytes
        bufferlist *outbl,   ///< [out] optional: reference the result here
        char *out);      ///< [out] optional: or copy it here
    int _read_random(
        FileReader *h,   ///< [in] read from here
        uint64_t offset, ///< [in] offset
        size_t len,      ///< [in] this many bytes
        char *out);      ///< [out] optional: or copy it here

    void _invalidate_cache(FileRef f, uint64_t offset, uint64_t length);

    int _open_super();
    int _write_super();
    int _replay(bool noop); ///< replay journal

    FileWriter *_create_writer(FileRef f);
    void _close_writer(FileWriter *h);

    // always put the super in the second 4k block.  FIXME should this be
    // block size independent?
    unsigned get_super_offset() {
        return 4096;
    }
    unsigned get_super_length() {
        return 4096;
    }

public:
    BlueFS(BlueFSContext* cct) : cct(cct),
        bdev(nullptr),
        ioc(nullptr),
        block_total(0) {}
    ~BlueFS();

    // the super is always stored on bdev 0
    int mkfs();
    int mount();
    void umount();

    void collect_metadata(std::map<std::string,std::string> *pm);
    uint64_t get_alloc_size() {
        return alloc_size;
    }
    int fsck();

    uint64_t get_used();
    uint64_t get_total();
    uint64_t get_free();
    void get_usage(std::pair<uint64_t,uint64_t> *usage); // [<free,total> ...]

    void dump_block_extents(std::ostream& out);

    int open_for_write(
        const std::string& dir,
        const std::string& file,
        FileWriter **h,
        bool overwrite);

    int open_for_read(
        const std::string& dir,
        const std::string& file,
        FileReader **h,
        bool random = false);

    void close_writer(FileWriter *h) {
        std::lock_guard<std::mutex> l(lock);
        _close_writer(h);
    }

    int rename(const std::string& old_dir, const std::string& old_file,
            const std::string& new_dir, const std::string& new_file);

    int readdir(const std::string& dirname, std::vector<std::string> *ls);

    int unlink(const std::string& dirname, const std::string& filename);
    int mkdir(const std::string& dirname);
    int rmdir(const std::string& dirname);

    bool dir_exists(const std::string& dirname);
    int stat(const std::string& dirname, const std::string& filename,
        uint64_t *size, utime_t *mtime);

    int lock_file(const std::string& dirname, const std::string& filename, FileLock **p);
    int unlock_file(FileLock *l);

    void flush_log();
    void compact_log();

    /// sync any uncommitted state to disk
    void sync_metadata();

    int add_block_device(const std::string& path);
    bool bdev_support_label();
    uint64_t get_block_device_size();

    /// gift more block space
    void add_block_extent(uint64_t offset, uint64_t len);

    /// reclaim block space
    int reclaim_blocks(uint64_t want, PExtentVector *extents);

    void flush(FileWriter *h) {
        std::lock_guard<std::mutex> l(lock);
        _flush(h, false);
    }
    int fsync(FileWriter *h) {
        std::unique_lock<std::mutex> l(lock);
        return _fsync(h, l);
    }
    int read(FileReader *h, FileReaderBuffer *buf, uint64_t offset, size_t len,
        bufferlist *outbl, char *out) {
        // no need to hold the global lock here; we only touch h and
        // h->file, and read vs write or delete is already protected (via
        // atomics and asserts).
        return _read(h, buf, offset, len, outbl, out);
    }
    int read_random(FileReader *h, uint64_t offset, size_t len,
            char *out) {
        // no need to hold the global lock here; we only touch h and
        // h->file, and read vs write or delete is already protected (via
        // atomics and asserts).
        return _read_random(h, offset, len, out);
    }

    void invalidate_cache(FileRef f, uint64_t offset, uint64_t len) {
        std::lock_guard<std::mutex> l(lock);
        _invalidate_cache(f, offset, len);
    }
    int preallocate(FileRef f, uint64_t offset, uint64_t len) {
        std::lock_guard<std::mutex> l(lock);
        return _preallocate(f, offset, len);
    }
};



#endif //BLUEFS_H