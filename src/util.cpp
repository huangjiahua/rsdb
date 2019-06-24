//
// Created by jiahua on 19-6-24.
//

#include "util.h"
#include <cerrno>        /* for definition of errno */
#include <cstdarg>        /* ISO C variable aruments */
#include <cstdlib>
#include <cstdio>
#include <cstring>

constexpr int MAX_LINE = 4096;

static void err_doit(int, int, const char *, va_list);

/*
 * Nonfatal error related to a system call.
 * Print a message and return.
 */
void
err_ret(const char *fmt, ...) {
    va_list ap;

    va_start(ap, fmt);
    err_doit(1, errno, fmt, ap);
    va_end(ap);
}

/*
 * Fatal error related to a system call.
 * Print a message and terminate.
 */
void
err_sys(const char *fmt, ...) {
    va_list ap;

    va_start(ap, fmt);
    err_doit(1, errno, fmt, ap);
    va_end(ap);
    exit(1);
}

/*
 * Nonfatal error unrelated to a system call.
 * Error code passed as explict parameter.
 * Print a message and return.
 */
void
err_cont(int error, const char *fmt, ...) {
    va_list ap;

    va_start(ap, fmt);
    err_doit(1, error, fmt, ap);
    va_end(ap);
}

/*
 * Fatal error unrelated to a system call.
 * Error code passed as explict parameter.
 * Print a message and terminate.
 */
void
err_exit(int error, const char *fmt, ...) {
    va_list ap;

    va_start(ap, fmt);
    err_doit(1, error, fmt, ap);
    va_end(ap);
    exit(1);
}

/*
 * Fatal error related to a system call.
 * Print a message, dump core, and terminate.
 */
void
err_dump(const char *fmt, ...) {
    va_list ap;

    va_start(ap, fmt);
    err_doit(1, errno, fmt, ap);
    va_end(ap);
    abort();        /* dump core and terminate */
    exit(1);        /* shouldn't get here */
}

/*
 * Nonfatal error unrelated to a system call.
 * Print a message and return.
 */
void
err_msg(const char *fmt, ...) {
    va_list ap;

    va_start(ap, fmt);
    err_doit(0, 0, fmt, ap);
    va_end(ap);
}

/*
 * Fatal error unrelated to a system call.
 * Print a message and terminate.
 */
void
err_quit(const char *fmt, ...) {
    va_list ap;

    va_start(ap, fmt);
    err_doit(0, 0, fmt, ap);
    va_end(ap);
    exit(1);
}

/*
 * Print a message and return to caller.
 * Caller specifies "errnoflag".
 */
static void
err_doit(int errnoflag, int error, const char *fmt, va_list ap) {
    char buf[MAX_LINE];

    vsnprintf(buf, MAX_LINE - 1, fmt, ap);
    if (errnoflag)
        snprintf(buf + strlen(buf), MAX_LINE - strlen(buf) - 1, ": %s",
                 strerror(error));
    strcat(buf, "\n");
    fflush(stdout);        /* in case stdout and stderr are the same */
    fputs(buf, stderr);
    fflush(nullptr);        /* flushes all stdio output streams */
}

int
lock_reg(int fd, int cmd, int type, off_t offset, int whence, off_t len)
{
    struct flock	lock;

    lock.l_type = type;		/* F_RDLCK, F_WRLCK, F_UNLCK */
    lock.l_start = offset;	/* byte offset, relative to l_whence */
    lock.l_whence = whence;	/* SEEK_SET, SEEK_CUR, SEEK_END */
    lock.l_len = len;		/* #bytes (0 means to EOF) */

    return(fcntl(fd, cmd, &lock));
}

pid_t
lock_test(int fd, int type, off_t offset, int whence, off_t len)
{
    struct flock	lock;

    lock.l_type = type;		/* F_RDLCK or F_WRLCK */
    lock.l_start = offset;	/* byte offset, relative to l_whence */
    lock.l_whence = whence;	/* SEEK_SET, SEEK_CUR, SEEK_END */
    lock.l_len = len;		/* #bytes (0 means to EOF) */

    if (fcntl(fd, F_GETLK, &lock) < 0)
        err_sys("fcntl error");

    if (lock.l_type == F_UNLCK)
        return(0);		/* false, region isn't locked by another proc */
    return(lock.l_pid);	/* true, return pid of lock owner */
}

