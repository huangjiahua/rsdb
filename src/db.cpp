//
// Created by jiahua on 19-6-24.
//

#include "rsdb/rsdb.h"
#include "db_impl.h"
#include "iterator_impl.h"


rsdb::DB::DB(const std::string &pathName, rsdb::OpenOptions options) :
        impl_(new DBImpl(pathName, options)) {
    if (!impl_->Valid())
        impl_.reset(nullptr); // Free
}

rsdb::DB::DB(rsdb::DB &&rhs) noexcept :
        impl_(std::move(rhs.impl_)) {}

rsdb::DB &rsdb::DB::operator=(rsdb::DB &&rhs) noexcept {
    impl_ = std::move(rhs.impl_);
    return *this;
}

bool rsdb::DB::Put(const std::string &key, const std::string &data, WriteOptions options) {
    Slice key_slice(key);
    Slice data_slice(data);
    return impl_->Put(key_slice, data_slice, options);
}

bool rsdb::DB::Get(const std::string &key, std::string *data) {
    Slice key_slice(key);
    Slice data_slice;
    bool ret = impl_->Get(key_slice, &data_slice);
    if (ret)
        *data = data_slice.ToString();
    return ret;
}

int rsdb::DB::Delete(const std::string &key) {
    Slice key_slice(key);
    return impl_->Delete(key_slice);
}

bool rsdb::DB::Valid() const {
    return impl_ != nullptr && impl_->Valid();
}

rsdb::Iterator &&rsdb::DB::Iterator() const {
    rsdb::Iterator iter;
    iter.impl_->db = impl_.get();
    return std::move(iter);
}

rsdb::DB::~DB() = default;




