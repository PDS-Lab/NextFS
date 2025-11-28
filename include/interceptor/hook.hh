#pragma once
#include "libsyscall_intercept_hook_point.h"
#include <sys/stat.h>
#include <sys/types.h>

namespace nextfs {

auto hook(long syscall_num, long a0, long a1, long a2, long a3, long a4,
          long a5, long *res) -> int;
auto hook_wrapper(long syscall_num, long a0, long a1, long a2, long a3, long a4,
                  long a5, long *res) -> int;

auto hook_mkdirat(int dirfd, const char *pathname, mode_t mode) -> int;
auto hook_openat(int dirfd, const char *pathname, int flags, mode_t mode)
    -> int;
auto hook_close(int fd) -> int;
auto hook_stat(const char *pathname, struct stat *buf) -> int;
auto hook_fstat(int fd, struct stat *buf) -> int;
auto hook_fsync(int fd) -> int;
auto hook_read(int fd, void *buf, size_t count) -> ssize_t;
auto hook_pread(int fd, void *buf, size_t count, off_t offset) -> ssize_t;
auto hook_write(int fd, const void *buf, size_t count) -> ssize_t;
auto hook_pwrite(int fd, const void *buf, size_t count, off_t offset)
    -> ssize_t;
auto hook_unlink(const char *pathname) -> int;
auto hook_lseek(int fd, off_t offset, int whence) -> int;
auto hook_rename(const char *oldpath, const char *newpath) -> int;
} // namespace nextfs
