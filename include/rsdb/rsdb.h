//
// Created by jiahua on 19-6-24.
//
#pragma once

#include <string>
#include <cstring>
#include <cassert>
#include <memory>
#include "rsdb/slice.h"

namespace rsdb {

struct OpenOptions {
    enum Type {
        OPEN, CREATE, OPEN_OR_CREATE, TRUNC
    };
    Type type = TRUNC;
    int mode = 0600;
};

struct WriteOptions {
    enum Type {
        INSERT, REPLACE, STORE
    };
    Type type = STORE;
};

class Iterator;

class DB {
    friend class Iterator;

public:
    DB(const std::string &pathName, OpenOptions options);

    ~DB();

    DB(const DB &rhs) = delete;

    DB &operator=(const DB &rhs) = delete;

    DB(DB &&rhs) noexcept;

    DB &operator=(DB &&rhs) noexcept;

    bool Put(const std::string &key, const std::string &data, WriteOptions options);

    bool Get(const std::string &key, std::string *data);

    int Delete(const std::string &key);

    bool Valid() const;

    Iterator GetIterator() const;

private:
    struct DBImpl;
    std::unique_ptr<DBImpl> impl_;
};

class Iterator {
    friend class DB;

public:
    void SeekToFirst();

    bool Valid() const;

    void Next();

    Slice Key();

    Slice Value();

    Iterator();

    Iterator(const Iterator &) = delete;

    Iterator &operator=(const Iterator &) = delete;

    Iterator(Iterator &&) noexcept;

    Iterator &operator=(Iterator &&) noexcept;

    ~Iterator();

private:
    struct IteratorImpl;
    std::unique_ptr<IteratorImpl> impl_;
};


} // namespace rsdb

