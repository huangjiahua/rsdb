//
// Created by jiahua on 19-6-24.
//

#pragma once

#include "rsdb/rsdb.h"
#include "util.h"

struct rsdb::DB::DBImpl {
    // Member Functions
    DBImpl(std::string pathName, OpenOptions options);

    ~DBImpl();

    int Put(const Slice &key, const Slice &data, WriteOptions options);

    int Get(const Slice &key, const Slice *data);

    int Delete(const Slice &key);

    // Predefined Data
    static constexpr size_t IDXLEN_SZ = 4;
    static constexpr char SEP = ':';
    static constexpr char SPACE = ' ';
    static constexpr char NEWLINE = '\n';

    static constexpr size_t PTR_SZ = 7;
    static constexpr size_t PTR_MAX = 9999999;
    static constexpr size_t NHASH_DEF = 137;
    static constexpr size_t FREE_OFF = 0;
    static constexpr size_t HASH_OFF = PTR_SZ;

    // Predefined Type
    using DBHASH = size_t;
    using COUNT = size_t;

    // Data Members
    int idxfd;  /* fd for index file */
    int datfd;  /* fd for data file */
    char *idxbuf; /* malloc'ed buffer for index record */
    char *datbuf; /* malloc'ed buffer for data record*/
    char *name;   /* name db was opened under */
    off_t idxoff; /* offset in index file of index record */
    /* key is at (idxoff + PTR_SZ + IDXLEN_SZ) */
    size_t idxlen; /* length of index record */
    /* excludes IDXLEN_SZ bytes at front of record */
    /* includes newline at end of index record */
    off_t datoff; /* offset in data file of data record */
    size_t datlen; /* length of data record */
    /* includes newline at end */
    off_t ptrval; /* contents of chain ptr in index record */
    off_t ptroff; /* chain ptr offset pointing to this idx record */
    off_t chainoff; /* offset of hash chain for this index record */
    off_t hashoff;  /* offset in index file of hash table */
    DBHASH nhash;    /* current hash table size */
    COUNT cnt_delok;    /* delete OK */
    COUNT cnt_delerr;   /* delete error */
    COUNT cnt_fetchok;  /* fetch OK */
    COUNT cnt_fetcherr; /* fetch error */
    COUNT cnt_nextrec;  /* nextrec */
    COUNT cnt_stor1;    /* store: DB_INSERT, no empty, appended */
    COUNT cnt_stor2;    /* store: DB_INSERT, found empty, reused */
    COUNT cnt_stor3;    /* store: DB_REPLACE, diff len, appended */
    COUNT cnt_stor4;    /* store: DB_REPLACE, same len, overwrote */
    COUNT cnt_storerr;  /* store error */
};




