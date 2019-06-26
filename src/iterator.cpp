#include "iterator_impl.h"

rsdb::Iterator::Iterator() : impl_(new IteratorImpl()) {}

rsdb::Iterator::~Iterator() {}

rsdb::Iterator::Iterator(Iterator &&rhs) noexcept :
        impl_(std::move(rhs.impl_)) {
}

rsdb::Iterator &rsdb::Iterator::operator=(Iterator &&rhs) noexcept {
    impl_ = std::move(rhs.impl_);
    return *this;
}

void rsdb::Iterator::SeekToFirst() {
    impl_->SeekToFirst();
}

bool rsdb::Iterator::Valid() const {
    return impl_->Valid();
}

void rsdb::Iterator::Next() {
    impl_->Next();
}

rsdb::Slice rsdb::Iterator::Key() {
    return impl_->Key();
}

rsdb::Slice rsdb::Iterator::Value() {
    return impl_->Value();
}

