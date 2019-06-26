#include "iterator_impl.h"

rsdb::Iterator::IteratorImpl::IteratorImpl() {
    if ((this->idxbuf = static_cast<char *>(malloc(DBImpl::IDXLEN_MAX)))
        == nullptr)
        err_dump("IteratorImpl(): malloc error");

    if ((this->datbuf = static_cast<char *>(malloc(DBImpl::DATLEN_MAX)))
        == nullptr)
        err_dump("IteratorImpl(): malloc error");
}

rsdb::Iterator::IteratorImpl::~IteratorImpl() {
    IteratorFree();
}

void rsdb::Iterator::IteratorImpl::IteratorFree() {
    if (this->idxbuf != nullptr) {
        free(this->idxbuf);
        idxbuf = nullptr;
    }
    if (this->datbuf != nullptr) {
        free(this->datbuf);
        datbuf = nullptr;
    }
}

void rsdb::Iterator::IteratorImpl::Rewind() {
    db->DBRewind();
    this->curroff = db->idxoff;
    this->nextoff = db->chainoff;
}

void rsdb::Iterator::IteratorImpl::SeekToFirst() {
    Rewind();
    int rc = db->DBNextTrec(this->idxbuf, this->datbuf, this->curroff, this->nextoff);
    this->valid = (rc >= 0);
}

void rsdb::Iterator::IteratorImpl::Next() {
    this->curroff = this->nextoff;
    int rc = db->DBNextTrec(this->idxbuf, this->datbuf, this->curroff, this->nextoff);
    this->valid = (rc >= 0);
}

rsdb::Slice rsdb::Iterator::IteratorImpl::Key() const {
    return Slice(this->idxbuf);
}

rsdb::Slice rsdb::Iterator::IteratorImpl::Value() const {
    return Slice(this->datbuf);
}

bool rsdb::Iterator::IteratorImpl::Valid() const {
    return valid;
}




