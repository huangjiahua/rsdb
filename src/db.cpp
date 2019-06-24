//
// Created by jiahua on 19-6-24.
//

#include "rsdb/rsdb.h"
#include "db_impl.h"


rsdb::DB::DB(const std::string &pathName, rsdb::OpenOptions options) :
        impl_(new DBImpl(pathName, options)) {}

rsdb::DB::DB(rsdb::DB &&rhs) noexcept :
        impl_(std::move(rhs.impl_)) {}

rsdb::DB &rsdb::DB::operator=(rsdb::DB &&rhs) noexcept {
    impl_ = std::move(rhs.impl_);
    return *this;
}

int rsdb::DB::Put(const std::string &key, const std::string &data, WriteOptions options) {
    Slice key_slice(key);
    Slice data_slice(data);
    return impl_->Put(key_slice, data_slice, options);
}

int rsdb::DB::Get(const std::string &key, const std::string *data) {
    Slice key_slice(key);
    Slice data_slice(*data);
    return impl_->Get(key_slice, &data_slice);
}

int rsdb::DB::Delete(const std::string &key) {
    Slice key_slice(key);
    return impl_->Delete(key_slice);
}



