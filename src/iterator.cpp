#include "iterator_impl.h"

rsdb::Iterator::Iterator() : impl_(nullptr) {}

rsdb::Iterator::~Iterator() {}

rsdb::Iterator::Iterator(Iterator &&rhs) noexcept :
        impl_(std::move(rhs.impl_)) {
}

rsdb::Iterator &rsdb::Iterator::operator=(Iterator &&rhs) noexcept {
    impl_ = std::move(rhs.impl_);
    return *this;
}

void rsdb::Iterator::SeekToFirst() {
}

bool rsdb::Iterator::Valid() const {
    return false;
}

void rsdb::Iterator::Next() {
}

rsdb::Slice rsdb::Iterator::Key() {
    return Slice();
}

rsdb::Slice rsdb::Iterator::Value() {
    return Slice();
}

