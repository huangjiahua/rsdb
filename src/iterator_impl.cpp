#include "iterator_impl.h"

rsdb::Iterator::IteratorImpl::IteratorImpl() = default;

rsdb::Iterator::IteratorImpl::~IteratorImpl() = default;

void rsdb::Iterator::IteratorImpl::Rewind() {
    db->DBRewind(&ct);
    this->curroff = ct.idxoff;
    this->nextoff = ct.chainoff;
}

void rsdb::Iterator::IteratorImpl::SeekToFirst() {
    Rewind();
    int rc = db->DBNextTrec(&ct, ct.idxbuf, ct.datbuf, this->curroff, this->nextoff);
    this->valid = (rc >= 0);
}

void rsdb::Iterator::IteratorImpl::Next() {
    this->curroff = this->nextoff;
    int rc = db->DBNextTrec(&ct, ct.idxbuf, ct.datbuf, this->curroff, this->nextoff);
    this->valid = (rc >= 0);
}

rsdb::Slice rsdb::Iterator::IteratorImpl::Key() const {
    return Slice(ct.idxbuf);
}

rsdb::Slice rsdb::Iterator::IteratorImpl::Value() const {
    return Slice(ct.datbuf);
}

bool rsdb::Iterator::IteratorImpl::Valid() const {
    return valid;
}




