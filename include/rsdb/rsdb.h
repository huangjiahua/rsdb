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

    ~DB() = default;

    DB(const DB &rhs) = delete;

    DB &operator=(const DB &rhs) = delete;

    DB(DB &&rhs) noexcept;

    DB &operator=(DB &&rhs) noexcept;

    int Put(const std::string &key, const std::string &data, WriteOptions options);

    int Get(const std::string &key, const std::string *data);

    int Delete(const std::string &key);

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

