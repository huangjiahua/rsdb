#pragma once

#include "rsdb/rsdb.h"
#include "db_impl.h"

struct rsdb::Iterator::IteratorImpl {
    using DBImpl = rsdb::DB::DBImpl;
    using DBContext = rsdb::DB::DBImpl::DBContext;
    DBImpl *db = nullptr;
    char *idxbuf = nullptr;
    char *datbuf = nullptr;
    off_t curroff = 0;
    off_t nextoff = 0;
    bool valid = false;
    DBContext ct;

    IteratorImpl();

    ~IteratorImpl();

    void Rewind();

    void SeekToFirst();

    void Next();

    Slice Key() const;

    Slice Value() const;

    bool Valid() const;
};

