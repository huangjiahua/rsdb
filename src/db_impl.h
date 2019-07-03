//
// Created by jiahua on 19-6-24.
//

#pragma once

#include "rsdb/rsdb.h"
#include "util.h"
#include "hash.h"
#include <mutex>
#include <atomic>
#include <vector>
#include <cerrno>
#include <fcntl.h>
#include <sys/stat.h>

struct rsdb::DB::DBImpl {
    // Predefined Type
    using DBHASH = size_t;
    using COUNT = std::atomic<size_t>;
    using lock_guard = std::lock_guard<std::mutex>;
    using unique_lock = std::unique_lock<std::mutex>;

    // Member Functions For Callers
    DBImpl(std::string pathName, OpenOptions options);

    ~DBImpl();

    bool Put(const Slice &key, const Slice &data, WriteOptions options);

    bool Get(const Slice &key, Slice *data);

    bool Delete(const Slice &key);

    bool Valid() const;

    struct DBContext {
        char *idxbuf = nullptr; /* malloc'ed buffer for index record */
        char *datbuf = nullptr; /* malloc'ed buffer for data record*/
        off_t idxoff = 0; /* offset in index file of index record */
        /* key is at (idxoff + PTR_SZ + IDXLEN_SZ) */
        size_t idxlen = 0; /* length of index record */
        /* excludes IDXLEN_SZ bytes at front of record */
        /* includes newline at end of index record */
        off_t datoff = 0; /* offset in data file of data record */
        size_t datlen = 0; /* length of data record */
        /* includes newline at end */
        off_t ptrval = 0; /* contents of chain ptr in index record */
        off_t ptroff = 0; /* chain ptr offset pointing to this idx record */
        off_t chainoff = 0; /* offset of hash chain for this index record */
        DBContext();

        ~DBContext();
    };

    // Member Functions for Self

    bool DBAlloc(size_t sz);

    void DBFree();

    void DBRewind(DBContext *ct);

    int DBNextTrec(DBContext *ct, char *key, char *ptr, off_t &curroffset, off_t &nextoffset);

    int DBFindAndLock(DBContext *ct, const Slice &key, bool writelock);

    char *DBReadDat(DBContext *ct);

    off_t DBReadPtr(DBContext *ct, off_t offset);

    off_t DBReadIdx(DBContext *ct, off_t offset);

    DBHASH DBHash(const Slice &s);

    void DBDoDelete(DBContext *ct);

    void DBWriteDat(DBContext *ct, const char *buf, off_t offset, int whence);

    void DBWriteIdx(DBContext *ct, const char *buf, off_t offset, int whence, off_t freeptr);

    void DBWritePtr(DBContext *ct, off_t offset, off_t saveptr);

    off_t DBFindFree(DBContext *ct, size_t keylen, size_t datlen);

    void OpenFiles(int flag, int mode, size_t namelen);


    // Predefined Value
    // the length of the "size of index"
    static constexpr size_t IDXLEN_SZ = 4;
    // the separator that separates fields of indexes
    static constexpr char SEP = ':';
    // empty space
    static constexpr char SPACE = ' ';
    // new line mark
    static constexpr char NEWLINE = '\n';
    // pointer length
    static constexpr size_t PTR_SZ = 7;
    // the maximum value a pointer can points, not useful
    static constexpr size_t PTR_MAX = 9999999;
    // default hash table size(how many buckets)
    static constexpr size_t NHASH_DEF = 137;
    // the offset of free list
    static constexpr size_t FREE_OFF = 0;
    // the offset of the first hash bucket
    static constexpr size_t HASH_OFF = PTR_SZ;
    // the minimum index length
    static constexpr size_t IDXLEN_MIN = 6;
    // the maximum index length, don't need to be this value
    static constexpr size_t IDXLEN_MAX = 4096;
    // the minimum data length
    static constexpr size_t DATLEN_MIN = 2;
    // the maximum data length, don't need to be this value
    static constexpr size_t DATLEN_MAX = 4096;

    std::vector<std::mutex> idx_muts;
    std::mutex idx_file_mut;
    std::mutex dat_file_mut;
    std::mutex idx_seek_mut;
    std::mutex dat_seek_mut;

    DBContext main_ct;
    // Data Members
    int idxfd = -1;  /* fd for index file */
    int datfd = -1;  /* fd for data file */
    char *idxbuf = nullptr; /* malloc'ed buffer for index record */
    char *datbuf = nullptr; /* malloc'ed buffer for data record*/
    char *name = nullptr;   /* name db was opened under */
    off_t hashoff = 0;  /* offset in index file of hash table */
    DBHASH nhash = 0;    /* current hash table size */
    COUNT cnt_delok{0};    /* delete OK */
    COUNT cnt_delerr{0};   /* delete error */
    COUNT cnt_fetchok{0};  /* fetch OK */
    COUNT cnt_fetcherr{0}; /* fetch error */
    COUNT cnt_nextrec{0};  /* nextrec */
    COUNT cnt_stor1{0};    /* store: DB_INSERT, no empty, appended */
    COUNT cnt_stor2{0};    /* store: DB_INSERT, found empty, reused */
    COUNT cnt_stor3{0};    /* store: DB_REPLACE, diff len, appended */
    COUNT cnt_stor4{0};    /* store: DB_REPLACE, same len, overwrote */
    COUNT cnt_storerr{0};  /* store error */
};

static constexpr char idx_extension[] = ".idx";
static constexpr char dat_extension[] = ".dat";



