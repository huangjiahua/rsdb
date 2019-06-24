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
    static constexpr int OPEN = 1;
    static constexpr int CREATE = 2;
    static constexpr int OPEN_OR_CREATE = 3;

    int type = OPEN_OR_CREATE;
    int mode = 0600;
};

struct WriteOptions {
    static constexpr int INSERT = 1;
    static constexpr int REPLACE = 2;
    static constexpr int STORE = 3;

    int type = STORE;
};

class DB {
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
};


} // namespace rsdb

