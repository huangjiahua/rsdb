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
    strcpy(this->name, pathName.c_str());
    strcat(this->name, idx_extension);
    idx_muts = std::vector<std::mutex>(this->nhash + 2);

    if (options.type == OpenOptions::CREATE) {
        OpenFiles(O_CREAT | O_EXCL | O_RDWR, options.mode, len);
    } else if (options.type == OpenOptions::OPEN_OR_CREATE) {
        OpenFiles(O_CREAT | O_TRUNC | O_RDWR, options.mode, len);
    } else {
        OpenFiles(O_RDWR, options.mode, len);
    }

    // check for open error
    if (!Valid()) {
        DBFree();
        return;
    }

    // if the database is created
    // initialize the database files
    if (options.type == OpenOptions::CREATE
        || options.type == OpenOptions::OPEN_OR_CREATE) {
        lock_guard lk(idx_file_mut);
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
    DBRewind(&main_ct);
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
    DBContext ct;

    lock_guard lk(idx_muts[DBHash(key) + 1]);
    if (DBFindAndLock(&ct, key, false) < 0) {
        ptr = nullptr;
        this->cnt_fetcherr++;
    } else {
        ptr = DBReadDat(&ct);
        this->cnt_fetchok++;
    }

    if (un_lock(this->idxfd, ct.chainoff, SEEK_SET, 1) < 0)
        err_dump("DBImpl::GET: un_lock error");

    *data = Slice(ptr);
    return (ptr != nullptr);
}


int rsdb::DB::DBImpl::DBFindAndLock(DBContext *ct, const rsdb::Slice &key, bool writelock) {
    off_t offset, nextoffset;

    ct->chainoff = (DBHash(key) * PTR_SZ) + this->hashoff;
    ct->ptroff = ct->chainoff;

    if (writelock) {
        if (writew_lock(this->idxfd, ct->chainoff, SEEK_SET, 1) < 0)
            err_dump("DBFindAndLock: writew_lock error");
    } else {
        if (readw_lock(this->idxfd, ct->chainoff, SEEK_SET, 1) < 0)
            err_dump("DBFindAndLock: readw_lock error");
    }

    offset = DBReadPtr(ct, ct->ptroff);

    while (offset != 0) {
        nextoffset = DBReadIdx(ct, offset);
        if (strcmp(ct->idxbuf, key.data()) == 0)
            break;
        ct->ptroff = offset;
        offset = nextoffset;
    }

    return (offset == 0 ? -1 : 0);
}

off_t rsdb::DB::DBImpl::DBReadPtr(DBContext *ct, off_t offset) {
    char asciiptr[PTR_SZ + 1];

    unique_lock lk(idx_seek_mut);
    if (lseek(this->idxfd, offset, SEEK_SET) == -1)
        err_dump("DBReadPtr: lseek error to ptr field");
    if (read(this->idxfd, asciiptr, PTR_SZ) != PTR_SZ)
        err_dump("DBReadPtr: read error of ptr field");
    lk.unlock();
    asciiptr[PTR_SZ] = '\0';
    return (atol(asciiptr));
}

off_t rsdb::DB::DBImpl::DBReadIdx(DBContext *ct, off_t offset) {
    ssize_t i;
    char *ptr1, *ptr2;
    char asciiptr[PTR_SZ + 1], asciilen[IDXLEN_SZ + 1];
    struct iovec iov[2];

    unique_lock lk(idx_seek_mut);
    if ((ct->idxoff = lseek(this->idxfd, offset,
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
    ct->ptrval = atol(asciiptr);

    asciilen[IDXLEN_SZ] = '\0';
    if ((ct->idxlen = atoi(asciilen)) < IDXLEN_MIN
        || ct->idxlen > IDXLEN_MAX)
        err_dump("DBReadIdx: invalid length");

    if ((i = read(this->idxfd, ct->idxbuf, ct->idxlen)) != ct->idxlen)
        err_dump("DBReadIdx: read error of index record");
    lk.unlock();
    if (ct->idxbuf[ct->idxlen - 1] != NEWLINE)
        err_dump("DBReadIdx: missing newline");
    ct->idxbuf[ct->idxlen - 1] = '\0';

    if ((ptr1 = strchr(ct->idxbuf, SEP)) == nullptr)
        err_dump("DBReadIdx: missing first separator");
    *ptr1++ = '\0';

    if ((ptr2 = strchr(ptr1, SEP)) == nullptr)
        err_dump("DBReadIdx: missing second separator");
    *ptr2++ = '\0';

    if (strchr(ptr2, SEP) != nullptr)
        err_dump("DBReadIdx: too many separators");

    if ((ct->datoff = atol(ptr1)) < 0)
        err_dump("DBReadIdx: starting offset < 0");
    if ((ct->datlen = atol(ptr2)) <= 0 || ct->datlen > DATLEN_MAX)
        err_dump("DBReadIdx: invalid length");
    return (ct->ptrval);
}

char *rsdb::DB::DBImpl::DBReadDat(DBContext *ct) {
    unique_lock lk(dat_seek_mut);
    if (lseek(this->datfd, ct->datoff, SEEK_SET) == -1)
        err_dump("DBReadDat: lseek error");
    if (read(this->datfd, ct->datbuf, ct->datlen) != ct->datlen)
        err_dump("DBReadDat: read error");
    lk.unlock();
    if (ct->datbuf[ct->datlen - 1] != NEWLINE)
        err_dump("DBReadAt: missing new line");
    ct->datbuf[ct->datlen - 1] = '\0';
    return (ct->datbuf);
}

int rsdb::DB::DBImpl::Delete(const rsdb::Slice &key) {
    int rc = 0;
    DBContext ct;

    lock_guard lk(idx_muts[DBHash(key) + 1]);
    if (DBFindAndLock(&ct, key, true) == 0) {
        DBDoDelete(&ct);
        this->cnt_delok++;
    } else {
        rc = -1;
        this->cnt_delerr++;
    }
    if (un_lock(this->idxfd, ct.chainoff, SEEK_SET, 1) < 0)
        err_dump("DBImpl::Delete: un_lock error");
    return (rc);
}

void rsdb::DB::DBImpl::DBDoDelete(DBContext *ct) {
    int i;
    char *ptr;
    off_t freeptr, saveptr;

    for (ptr = ct->datbuf, i = 0; i < ct->datlen - 1; i++)
        *ptr++ = SPACE;
    *ptr = '\0';
    ptr = ct->idxbuf;
    assert(ptr[ct->idxlen - 1] == '\0');
    while (*ptr)
        *ptr++ = SPACE;

    lock_guard lk(idx_muts[0]);
    if (writew_lock(this->idxfd, FREE_OFF, SEEK_SET, true) < 0)
        err_dump("DBDoDelete: writew_lock error");

    DBWriteDat(ct, ct->datbuf, ct->datoff, SEEK_SET);

    freeptr = DBReadPtr(ct, FREE_OFF);

    saveptr = ct->ptrval;

    DBWriteIdx(ct, ct->idxbuf, ct->idxoff, SEEK_SET, freeptr);

    DBWritePtr(ct, FREE_OFF, ct->idxoff);

    DBWritePtr(ct, ct->ptroff, saveptr);
    if (un_lock(this->idxfd, FREE_OFF, SEEK_SET, 1) < 0)
        err_dump("DBDoDelete: un_lock error");
}

void rsdb::DB::DBImpl::DBWriteDat(DBContext *ct, const char *data, off_t offset, int whence) {
    struct iovec iov[2];
    static char newline = NEWLINE;

    unique_lock lk(dat_file_mut, std::defer_lock);
    if (whence == SEEK_END) {
        lk.lock();
        if (writew_lock(this->datfd, 0, SEEK_SET, 0) < 0)
            err_dump("DBWriteDat: writew_lock error");
    }

    unique_lock lk2(dat_seek_mut);
    if ((ct->datoff = lseek(this->datfd, offset, whence)) == -1)
        err_dump("DBWriteDat: lseek error");
    ct->datlen = strlen(data) + 1;

    iov[0].iov_base = (char *) data;
    iov[0].iov_len = ct->datlen - 1;
    iov[1].iov_base = &newline;
    iov[1].iov_len = 1;

    if (writev(this->datfd, &iov[0], 2) != ct->datlen)
        err_dump("DBWriteDat: writev error of data record");
    lk2.unlock();
    if (whence == SEEK_END)
        if (un_lock(this->datfd, 0, SEEK_SET, 0) < 0)
            err_dump("DBWriteDat: un_lock error");
}

void rsdb::DB::DBImpl::DBWriteIdx(DBContext *ct, const char *key, off_t offset, int whence, off_t ptrval) {
    struct iovec iov[2];
    char asciiptrlen[PTR_SZ + IDXLEN_SZ + 1];
    int len;

    if ((ct->ptrval = ptrval) < 0 || ptrval > PTR_MAX)
        err_quit("DBWriteIdx: invalid ptr: %d", ptrval);
    sprintf(ct->idxbuf, "%s%c%lld%c%ld\n", key, SEP,
            (long long) ct->datoff, SEP, (long) ct->datlen);
    len = strlen(ct->idxbuf);
    if (len < IDXLEN_MIN || len > IDXLEN_MAX)
        err_dump("DBWriteIdx: invalid length");
    sprintf(asciiptrlen, "%*lld%*d", (int) PTR_SZ, (long long) ptrval,
            (int) IDXLEN_SZ, len);

    unique_lock lk(idx_muts.back(), std::defer_lock);
    if (whence == SEEK_END) {
        lk.lock();
        if (writew_lock(this->idxfd, ((this->nhash + 1) * PTR_SZ) + 1,
                        SEEK_SET, 0) < 0)
            err_dump("DBWriteIdx: writew_lock error");
    }

    unique_lock lk2(idx_seek_mut);
    if ((ct->idxoff = lseek(this->idxfd, offset, whence)) == -1)
        err_dump("DBWriteIdx: lseek error");

    iov[0].iov_base = asciiptrlen;
    iov[0].iov_len = PTR_SZ + IDXLEN_SZ;
    iov[1].iov_base = ct->idxbuf;
    iov[1].iov_len = len;
    if (writev(this->idxfd, &iov[0], 2) != PTR_SZ + IDXLEN_SZ + len)
        err_dump("DBWriteIdx: writev error of index record");
    lk2.unlock();

    if (whence == SEEK_END)
        if (un_lock(this->idxfd, ((this->nhash + 1) * PTR_SZ) + 1,
                    SEEK_SET, 0) < 0)
            err_dump("DBWriteIdx: un_lock error");
}

void rsdb::DB::DBImpl::DBWritePtr(DBContext *ct, off_t offset, off_t ptrval) {
    char asciiptr[PTR_SZ + 1];

    if (ptrval < 0 || ptrval > PTR_MAX)
        err_quit("DBWritePtr: invalid ptr: %d", ptrval);
    sprintf(asciiptr, "%*lld", (int) PTR_SZ, (long long) ptrval);

    lock_guard lk(idx_seek_mut);
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
    DBContext ct;

    keylen = key.size();
    datlen = data.size();
    if (datlen < DATLEN_MIN || datlen > DATLEN_MAX)
        err_dump("DBImpl::Put: invalid data length");

    lock_guard lk(idx_muts[DBHash(key) + 1]);
    if (DBFindAndLock(&ct, key, true) < 0) {
        if (options.type == WriteOptions::REPLACE) {
            rc = false;
            this->cnt_storerr++;
            errno = ENOENT;
            goto doreturn;
        }
        ptrval = DBReadPtr(&ct, ct.chainoff);

        if (DBFindFree(&ct, keylen, datlen) < 0) {
            DBWriteDat(&ct, data.data(), 0, SEEK_END);
            DBWriteIdx(&ct, key.data(), 0, SEEK_END, ptrval);
            DBWritePtr(&ct, ct.chainoff, ct.idxoff);
            this->cnt_stor1++;
        } else {
            DBWriteDat(&ct, data.data(), ct.datoff, SEEK_SET);
            DBWriteIdx(&ct, key.data(), ct.idxoff, SEEK_SET, ptrval);
            DBWritePtr(&ct, ct.chainoff, ct.chainoff);
            this->cnt_stor2++;
        }
    } else {
        if (options.type == WriteOptions::INSERT) {
            rc = false;
            this->cnt_storerr++;
            goto doreturn;
        }
        if (datlen != ct.datlen) {
            DBDoDelete(&ct);
            ptrval = DBReadPtr(&ct, ct.chainoff);
            DBWriteDat(&ct, data.data(), 0, SEEK_END);
            DBWriteIdx(&ct, key.data(), 0, SEEK_END, ptrval);
            DBWritePtr(&ct, ct.chainoff, ct.idxoff);
            this->cnt_stor3++;
        } else {
            DBWriteDat(&ct, data.data(), ct.datoff, SEEK_SET);
            this->cnt_stor4++;
        }
    }
    rc = true;
    doreturn:
    if (un_lock(this->idxfd, ct.chainoff, SEEK_SET, 1) < 0)
        err_dump("DBImpl::Put: unlock error");
    return rc;
}

off_t rsdb::DB::DBImpl::DBFindFree(DBContext *ct, size_t keylen, size_t datlen) {
    int rc;
    off_t offset, nextoffset, saveoffset;

    lock_guard lk(idx_muts[0]);
    if (writew_lock(this->idxfd, FREE_OFF, SEEK_SET, 1) < 0)
        err_dump("DBFindFree: writew_lock error");

    saveoffset = FREE_OFF;
    offset = DBReadPtr(ct, saveoffset);

    while (offset != 0) {
        nextoffset = DBReadIdx(ct, offset);
        if (strlen(ct->idxbuf) == keylen && ct->datlen == datlen)
            break;
        saveoffset = offset;
        offset = nextoffset;
    }

    if (offset == 0) {
        rc = -1;
    } else {
        DBWritePtr(ct, saveoffset, ct->ptrval);
        rc = 0;
    }

    if (un_lock(this->idxfd, FREE_OFF, SEEK_SET, 1) < 0)
        err_dump("DBFindFree: un_lock error");
    return (rc);
}


void rsdb::DB::DBImpl::DBRewind(DBContext *ct) {
    off_t offset;

    offset = (this->nhash + 1) * PTR_SZ;

    lock_guard lk(idx_seek_mut);
    if ((ct->idxoff = lseek(this->idxfd, offset + 1, SEEK_SET)) == -1)
        err_dump("DBRewind: lseek error");
}

int rsdb::DB::DBImpl::DBNextTrec(DBContext *ct, char *key, char *dat, off_t &curroffset, off_t &nextoffset) {
    char c;
    char *ptr;
    int rc;

    lock_guard lk(idx_muts[0]);
    if (readw_lock(this->idxfd, FREE_OFF, SEEK_SET, 1) < 0)
        err_dump("DBNextTrec: readw_lock error");

    ct->idxoff = curroffset;
    do {
        if (DBReadIdx(ct, 0) < 0) {
            rc = -1;
            goto doreturn;
        }

        ptr = ct->idxbuf;
        while ((c = *ptr++) != 0 && c == SPACE);
    } while (c == 0);

    nextoffset = curroffset + ct->idxlen + PTR_SZ + IDXLEN_SZ;

    if (key != nullptr)
        strcpy(key, ct->idxbuf);
    ptr = DBReadDat(ct);
    strcpy(dat, ptr);
    rc = 0;
    doreturn:
    if (un_lock(this->idxfd, FREE_OFF, SEEK_SET, 1) < 0)
        err_dump("DBNextTrek: un_lock error");
    return rc;
}

rsdb::DB::DBImpl::DBContext::DBContext() {
    if ((idxbuf = static_cast<char *>(malloc(IDXLEN_MAX + 2))) == nullptr)
        err_dump("DBContext(): malloc error for index buffer");
    if ((datbuf = static_cast<char *>(malloc(DATLEN_MAX))) == nullptr)
        err_dump("DBContext(): malloc error for data buffer");
}

rsdb::DB::DBImpl::DBContext::~DBContext() {
    free(idxbuf);
    free(datbuf);
}
