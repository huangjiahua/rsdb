//
// Created by jiahua on 19-6-24.
//

#include "db_impl.h"
#include "util.h"
#include <cstdlib>
#include <cassert>
#include <sys/uio.h>


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
            sprintf(asciiptr, "%*d", (int) PTR_SZ, 0);
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


bool rsdb::DB::DBImpl::Get(const rsdb::Slice &key, rsdb::Slice *data) {
    char *ptr;

    if (DBFindAndLock(key, false) < 0) {
        ptr = nullptr;
        this->cnt_fetcherr++;
    } else {
        ptr = DBReadDat();
        this->cnt_fetchok++;
    }

    if (un_lock(this->idxfd, this->chainoff, SEEK_SET, 1) < 0)
        err_dump("DBImpl::GET: un_lock error");

    *data = Slice(ptr);
    return (ptr != nullptr);
}


int rsdb::DB::DBImpl::DBFindAndLock(const rsdb::Slice &key, bool writelock) {
    off_t offset, nextoffset;

    this->chainoff = (DBHash(key) * PTR_SZ) + this->hashoff;
    this->ptroff = this->chainoff;

    if (writelock) {
        if (writew_lock(this->idxfd, this->chainoff, SEEK_SET, 1) < 0)
            err_dump("DBFindAndLock: writew_lock error");
    } else {
        if (readw_lock(this->idxfd, this->chainoff, SEEK_SET, 1) < 0)
            err_dump("DBFindAndLock: readw_lock error");
    }

    offset = DBReadPtr(this->ptroff);

    while (offset != 0) {
        nextoffset = DBReadIdx(offset);
        if (strcmp(this->idxbuf, key.data()) == 0)
            break;
        this->ptroff = offset;
        offset = nextoffset;
    }

    return (offset == 0 ? -1 : 0);
}

off_t rsdb::DB::DBImpl::DBReadPtr(off_t offset) {
    char asciiptr[PTR_SZ + 1];

    if (lseek(this->idxfd, offset, SEEK_SET) == -1)
        err_dump("DBReadPtr: lseek error to ptr field");
    if (read(this->idxfd, asciiptr, PTR_SZ) != PTR_SZ)
        err_dump("DBReadPtr: read error of ptr field");
    asciiptr[PTR_SZ] = '\0';
    return (atol(asciiptr));
}

off_t rsdb::DB::DBImpl::DBReadIdx(off_t offset) {
    ssize_t i;
    char *ptr1, *ptr2;
    char asciiptr[PTR_SZ + 1], asciilen[IDXLEN_SZ + 1];
    struct iovec iov[2];

    if ((this->idxoff = lseek(this->idxfd, offset,
                              offset == 0 ? SEEK_CUR : SEEK_SET)) == -1)
        err_dump("DBReadIdx: lseek error");

    iov[0].iov_base = asciiptr;
    iov[0].iov_len = PTR_SZ;
    iov[1].iov_base = asciilen;
    iov[1].iov_len = IDXLEN_SZ;

    if ((i = readv(this->idxfd, &iov[0], 2)) != PTR_SZ + IDXLEN_SZ) {
        if (i == 0 && offset == 0)
            return (-1); // EOF for Iterator Next
        err_dump("DBReadIdx: readv error of index record");
    }

    asciiptr[PTR_SZ] = '\0';
    this->ptrval = atol(asciiptr);

    asciilen[IDXLEN_SZ] = '\0';
    if ((this->idxlen = atoi(asciilen)) < IDXLEN_MIN
        || this->idxlen > IDXLEN_MAX)
        err_dump("DBReadIdx: invalid length");

    if ((i = read(this->idxfd, this->idxbuf, this->idxlen)) != this->idxlen)
        err_dump("DBReadIdx: read error of index record");
    if (this->idxbuf[this->idxlen - 1] != NEWLINE)
        err_dump("DBReadIdx: missing newline");
    this->idxbuf[this->idxlen - 1] = '\0';

    if ((ptr1 = strchr(this->idxbuf, SEP)) == nullptr)
        err_dump("DBReadIdx: missing first separator");
    *ptr1++ = '\0';

    if ((ptr2 = strchr(ptr1, SEP)) == nullptr)
        err_dump("DBReadIdx: missing second separator");
    *ptr2++ = '\0';

    if (strchr(ptr2, SEP) != nullptr)
        err_dump("DBReadIdx: too many separators");

    if ((this->datoff = atol(ptr1)) < 0)
        err_dump("DBReadIdx: starting offset < 0");
    if ((this->datlen = atol(ptr2)) <= 0 || this->datlen > DATLEN_MAX)
        err_dump("DBReadIdx: invalid length");
    return (this->ptrval);
}

char *rsdb::DB::DBImpl::DBReadDat() {
    if (lseek(this->datfd, this->datoff, SEEK_SET) == -1)
        err_dump("DBReadDat: lseek error");
    if (read(this->datfd, this->datbuf, this->datlen) != this->datlen)
        err_dump("DBReadDat: read error");
    if (this->datbuf[this->datlen - 1] != NEWLINE)
        err_dump("DBReadAt: missing new line");
    this->datbuf[this->datlen - 1] = '\0';
    return (this->datbuf);
}

int rsdb::DB::DBImpl::Delete(const rsdb::Slice &key) {
    int rc = 0;

    if (DBFindAndLock(key, true) == 0) {
        DBDoDelete();
        this->cnt_delok++;
    } else {
        rc = -1;
        this->cnt_delerr++;
    }
    if (un_lock(this->idxfd, this->chainoff, SEEK_SET, 1) < 0)
        err_dump("DBImpl::Delete: un_lock error");
    return (rc);
}

void rsdb::DB::DBImpl::DBDoDelete() {
    int i;
    char *ptr;
    off_t freeptr, saveptr;

    for (ptr = this->datbuf, i = 0; i < this->datlen - 1; i++)
        *ptr++ = SPACE;
    *ptr = '\0';
    ptr = this->idxbuf;
    assert(ptr[this->idxlen - 1] == '\0');
    while (*ptr)
        *ptr++ = SPACE;

    if (writew_lock(this->idxfd, FREE_OFF, SEEK_SET, true) < 0)
        err_dump("DBDoDelete: writew_lock error");

    DBWriteDat(this->datbuf, this->datoff, SEEK_SET);

    freeptr = DBReadPtr(FREE_OFF);

    saveptr = this->ptrval;

    DBWriteIdx(this->idxbuf, this->idxoff, SEEK_SET, freeptr);

    DBWritePtr(FREE_OFF, this->idxoff);

    DBWritePtr(this->ptroff, saveptr);
    if (un_lock(this->idxfd, FREE_OFF, SEEK_SET, 1) < 0)
        err_dump("DBDoDelete: un_lock error");
}

void rsdb::DB::DBImpl::DBWriteDat(const char *data, off_t offset, int whence) {
    struct iovec iov[2];
    static char newline = NEWLINE;

    if (whence == SEEK_END)
        if (writew_lock(this->datfd, 0, SEEK_SET, 0) < 0)
            err_dump("DBWriteDat: writew_lock error");

    if ((this->datoff = lseek(this->datfd, offset, whence)) == -1)
        err_dump("DBWriteDat: lseek error");
    this->datlen = strlen(data) + 1;

    iov[0].iov_base = (char *) data;
    iov[0].iov_len = this->datlen - 1;
    iov[1].iov_base = &newline;
    iov[1].iov_len = 1;

    if (writev(this->datfd, &iov[0], 2) != this->datlen)
        err_dump("DBWriteDat: writev error of data record");

    if (whence == SEEK_END)
        if (un_lock(this->datfd, 0, SEEK_SET, 0) < 0)
            err_dump("DBWriteDat: un_lock error");
}

void rsdb::DB::DBImpl::DBWriteIdx(const char *key, off_t offset, int whence, off_t ptrval) {
    struct iovec iov[2];
    char asciiptrlen[PTR_SZ + IDXLEN_SZ + 1];
    int len;

    if ((this->ptrval = ptrval) < 0 || ptrval > PTR_MAX)
        err_quit("DBWriteIdx: invalid ptr: %d", ptrval);
    sprintf(this->idxbuf, "%s%c%lld%c%ld\n", key, SEP,
            (long long) this->datoff, SEP, (long) this->datlen);
    len = strlen(this->idxbuf);
    if (len < IDXLEN_MIN || len > IDXLEN_MAX)
        err_dump("DBWriteIdx: invalid length");
    sprintf(asciiptrlen, "%*lld%*d", (int) PTR_SZ, (long long) ptrval,
            (int) IDXLEN_SZ, len);

    if (whence == SEEK_END)
        if (writew_lock(this->idxfd, ((this->nhash + 1) * PTR_SZ) + 1,
                        SEEK_SET, 0) < 0)
            err_dump("DBWriteIdx: writew_lock error");

    if ((this->idxoff = lseek(this->idxfd, offset, whence)) == -1)
        err_dump("DBWriteIdx: lseek error");

    iov[0].iov_base = asciiptrlen;
    iov[0].iov_len = PTR_SZ + IDXLEN_SZ;
    iov[1].iov_base = this->idxbuf;
    iov[1].iov_len = len;
    if (writev(this->idxfd, &iov[0], 2) != PTR_SZ + IDXLEN_SZ + len)
        err_dump("DBWriteIdx: writev error of index record");

    if (whence == SEEK_END)
        if (un_lock(this->idxfd, ((this->nhash + 1) * PTR_SZ) + 1,
                    SEEK_SET, 0) < 0)
            err_dump("DBWriteIdx: un_lock error");
}

void rsdb::DB::DBImpl::DBWritePtr(off_t offset, off_t ptrval) {
    char asciiptr[PTR_SZ + 1];

    if (ptrval < 0 || ptrval > PTR_MAX)
        err_quit("DBWritePtr: invalid ptr: %d", ptrval);
    sprintf(asciiptr, "%*lld", (int) PTR_SZ, (long long) ptrval);

    if (lseek(this->idxfd, offset, SEEK_SET) == -1)
        err_dump("DBWritePtr: lseek error to ptr field");
    if (write(this->idxfd, asciiptr, PTR_SZ) != PTR_SZ)
        err_dump("DBWritePtr: write error of ptr field");
}

inline rsdb::DB::DBImpl::DBHASH rsdb::DB::DBImpl::DBHash(const rsdb::Slice &s) {
    return Hash(s.data(), s.size()) % this->nhash;
}

bool rsdb::DB::DBImpl::Put(const rsdb::Slice &key, const rsdb::Slice &data, rsdb::WriteOptions options) {
    int keylen, datlen;
    bool rc = true;
    off_t ptrval;

    keylen = key.size();
    datlen = data.size();
    if (datlen < DATLEN_MIN || datlen > DATLEN_MAX)
        err_dump("DBImpl::Put: invalid data length");

    if (DBFindAndLock(key, true) < 0) {
        if (options.type == WriteOptions::REPLACE) {
            rc = false;
            this->cnt_storerr++;
            errno = ENOENT;
            goto doreturn;
        }
        ptrval = DBReadPtr(this->chainoff);

        if (DBFindFree(keylen, datlen) < 0) {
            DBWriteDat(data.data(), 0, SEEK_END);
            DBWriteIdx(key.data(), 0, SEEK_END, ptrval);
            DBWritePtr(this->chainoff, this->idxoff);
            this->cnt_stor1++;
        } else {
            DBWriteDat(data.data(), this->datoff, SEEK_SET);
            DBWriteIdx(key.data(), this->idxoff, SEEK_SET, ptrval);
            DBWritePtr(this->chainoff, this->chainoff);
            this->cnt_stor2++;
        }
    } else {
        if (options.type == WriteOptions::INSERT) {
            rc = false;
            this->cnt_storerr++;
            goto doreturn;
        }
        if (datlen != this->datlen) {
            DBDoDelete();
            ptrval = DBReadPtr(this->chainoff);
            DBWriteDat(data.data(), 0, SEEK_END);
            DBWriteIdx(key.data(), 0, SEEK_END, ptrval);
            DBWritePtr(this->chainoff, this->idxoff);
            this->cnt_stor3++;
        } else {
            DBWriteDat(data.data(), this->datoff, SEEK_SET);
            this->cnt_stor4++;
        }
    }
    rc = true;
    doreturn:
    if (un_lock(this->idxfd, this->chainoff, SEEK_SET, 1) < 0)
        err_dump("DBImpl::Put: unlock error");
    return rc;
}

off_t rsdb::DB::DBImpl::DBFindFree(size_t keylen, size_t datlen) {
    int rc;
    off_t offset, nextoffset, saveoffset;

    if (writew_lock(this->idxfd, FREE_OFF, SEEK_SET, 1) < 0)
        err_dump("DBFindFree: writew_lock error");

    saveoffset = FREE_OFF;
    offset = DBReadPtr(saveoffset);

    while (offset != 0) {
        nextoffset = DBReadIdx(offset);
        if (strlen(this->idxbuf) == keylen && this->datlen == datlen)
            break;
        saveoffset = offset;
        offset = nextoffset;
    }

    if (offset == 0) {
        rc = -1;
    } else {
        DBWritePtr(saveoffset, this->ptrval);
        rc = 0;
    }

    if (un_lock(this->idxfd, FREE_OFF, SEEK_SET, 1) < 0)
        err_dump("DBFindFree: un_lock error");
    return (rc);
}


void rsdb::DB::DBImpl::DBRewind() {
    off_t offset;

    offset = (this->nhash + 1) * PTR_SZ;

    if ((this->idxoff = lseek(this->idxfd, offset + 1, SEEK_SET)) == -1)
        err_dump("DBRewind: lseek error");
}

int rsdb::DB::DBImpl::DBNextTrec(char *key, char *dat, off_t &curroffset, off_t &nextoffset) {
    char c;
    char *ptr;
    int rc;

    if (readw_lock(this->idxfd, FREE_OFF, SEEK_SET, 1) < 0)
        err_dump("DBNextTrec: readw_lock error");

    this->idxoff = curroffset;
    do {
        if (DBReadIdx(0) < 0) {
            rc = -1;
            goto doreturn;
        }

        ptr = this->idxbuf;
        while ((c = *ptr++) != 0 && c == SPACE);
    } while (c == 0);

    nextoffset = curroffset + this->idxlen + PTR_SZ + IDXLEN_SZ;

    if (key != nullptr)
        strcpy(key, this->idxbuf);
    ptr = DBReadDat();
    strcpy(dat, ptr);
    rc = 0;
    doreturn:
    if (un_lock(this->idxfd, FREE_OFF, SEEK_SET, 1) < 0)
        err_dump("DBNextTrek: un_lock error");
    return rc;
}
