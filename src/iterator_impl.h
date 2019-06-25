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

    IteratorImpl();
    ~IteratorImpl();

    void Rewind();

    void IteratorFree();
};

