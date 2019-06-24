//
// Created by jiahua on 19-6-24.
//

#include "db_impl.h"
#include "util.h"
#include <cstdlib>
#include <cassert>


rsdb::DB::DBImpl::DBImpl(std::string pathName, rsdb::OpenOptions options) {
    size_t i;
    size_t len;
    char asciiptr[PTR_SZ + 1];
    char hash[(NHASH_DEF + 1) * PTR_SZ + 2];
    struct stat statbuf;

    len = pathName.size();
    if (!DBAlloc(len))
        err_dump("DbImpl(): DBAlloc error for DBImpl");

    this->nhash = NHASH_DEF;
    this->hashoff = HASH_OFF;
    strcpy(this->name, pathName.c_str());
    strcat(this->name, idx_extension);

    if (options.type == OpenOptions::CREATE) {
        OpenFiles(O_CREAT | O_EXCL | O_RDWR, options.mode, len);
        this->err_msg = "DbImpl(): open O_CREAT error";
    } else if (options.type == OpenOptions::OPEN_OR_CREATE) {
        OpenFiles(O_CREAT | O_TRUNC | O_RDWR, options.mode, len);
        this->err_msg = "DbImpl(): open O_TRUNC error";
    } else {
        OpenFiles(O_RDWR, options.mode, len);
        this->err_msg = "DbImpl(): open O_RDWR error";
    }

    // check for open error
    if (!Valid()) {
        DBFree();
        return;
    }
    this->err_msg.clear();

    // if the database is created
    // initialize the database files
    if (options.type == OpenOptions::CREATE
        || options.type == OpenOptions::OPEN_OR_CREATE) {
        if (writew_lock(this->idxfd, 0, SEEK_SET, 0) < 0)
            err_dump("DBImpl(): writew_lock error");

        if (fstat(this->idxfd, &statbuf) < 0)
            err_sys("DBImpl(): fstat error");

        if (statbuf.st_size == 0) {
            sprintf(asciiptr, "%*d", (int)PTR_SZ, 0);
            hash[0] = 0;
            for (i = 0; i < this->nhash + 1; i++)
                strcat(hash, asciiptr);
            strcat(hash, "\n");
            i = strlen(hash);
            if (write(this->idxfd, hash, i) != i)
                err_dump("DBImpl(): index file init write error");
        }
        if (un_lock(this->idxfd, 0, SEEK_SET, 0) < 0)
            err_dump("DBImpl(): un_lock error");
    }
    DBRewind();
}

inline bool rsdb::DB::DBImpl::Valid() const {
    return this->idxfd >= 0 && this->datfd >= 0;
}

bool rsdb::DB::DBImpl::DBAlloc(size_t namelen) {
    assert(this->idxfd == -1 && this->datfd == -1);

    if ((this->name = reinterpret_cast<char *>(malloc(namelen)))
        == nullptr)
        err_dump("DBAlloc: malloc error for name");

    if ((this->idxbuf = reinterpret_cast<char *>(malloc(IDXLEN_MAX + 2)))
        == nullptr)
        err_dump("DBAlloc: malloc error for index buffer");

    if ((this->datbuf = reinterpret_cast<char *>(malloc(DATLEN_MAX)))
        == nullptr)
        err_dump("DBAlloc: malloc error for data buffer");
    return true;
}

inline void rsdb::DB::DBImpl::OpenFiles(int flag, int mode, size_t namelen) {
    this->idxfd = open(this->name, flag, mode);
    strcpy(this->name + namelen, dat_extension);
    this->datfd = open(this->name, flag, mode);
}

rsdb::DB::DBImpl::~DBImpl() {
    DBFree();
}

void rsdb::DB::DBImpl::DBFree() {
    if (this->idxfd >= 0) {
        close(this->idxfd);
        this->idxfd = 0;
    }
    if (this->datfd >= 0) {
        close(this->datfd);
        this->datfd = 0;
    }
    if (this->idxbuf != nullptr) {
        free(this->idxbuf);
        this->idxbuf = nullptr;
    }
    if (this->datbuf != nullptr) {
        free(this->datbuf);
        this->datbuf = nullptr;
    }
    if (this->name != nullptr) {
        free(this->name);
        this->name = nullptr;
    }
}

void rsdb::DB::DBImpl::DBRewind() {

}

bool rsdb::DB::DBImpl::Put(const rsdb::Slice &key, const rsdb::Slice &data, rsdb::WriteOptions options) {
    return false;
}

bool rsdb::DB::DBImpl::Get(const rsdb::Slice &key, rsdb::Slice *data) {
    char *ptr;

    if (DBFindAndLock(key, false) < 0) {
        ptr = nullptr;
        this->cnt_fetcherr++;
    } else {
        ptr = DBReadAt();
        this->cnt_fetchok++;
    }

    if (un_lock(this->idxfd, this->chainoff, SEEK_SET, 1) < 0)
        err_dump("DBImpl::GET: un_lock error");

    *data = Slice(ptr);
    return (ptr != nullptr);
}

int rsdb::DB::DBImpl::Delete(const rsdb::Slice &key) {
    return 0;
}

int rsdb::DB::DBImpl::DBFindAndLock(const rsdb::Slice &key, bool writelock) {
    return 0;
}

char *rsdb::DB::DBImpl::DBReadAt() {
    return nullptr;
}




