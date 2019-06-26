#pragma once

#include "rsdb/rsdb.h"
#include "db_impl.h"

struct rsdb::Iterator::IteratorImpl {
    using DBImpl = rsdb::DB::DBImpl;
    DBImpl *db = nullptr;
    char *idxbuf = nullptr;
    char *datbuf = nullptr;
    off_t curroff = 0;
    off_t nextoff = 0;
    bool valid = false;

    IteratorImpl();

    ~IteratorImpl();

    void Rewind();

    void SeekToFirst();

    void Next();

    Slice Key() const;

    Slice Value() const;

    bool Valid() const;

    void IteratorFree();
};

