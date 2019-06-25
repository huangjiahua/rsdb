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


