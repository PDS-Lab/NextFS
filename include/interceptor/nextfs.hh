#pragma once
#include "interceptor/file.hh"
#include <sys/types.h>

namespace nextfs {
auto nextfs_mkdirat(int dirfd, std::string_view pathname, mode_t mode) -> int;
// auto nextfs_fsync(const std::shared_ptr<FileInfo> &file) -> int;
auto nextfs_openat(std::string_view pathname, int flags, mode_t mode) -> int;
auto nextfs_close(const std::shared_ptr<FileInfo> &file) -> int;
auto nextfs_rename(std::string_view old_pathname, std::string_view new_pathname)
    -> int;
auto nextfs_stat(std::string_view pathname, struct stat *buf) -> int;
auto nextfs_fstat(const std::shared_ptr<FileInfo> &file, struct stat *buf)
    -> int;
auto nextfs_read(const std::shared_ptr<FileInfo> &file, void *buf, size_t count)
    -> ssize_t;
auto nextfs_pread(const std::shared_ptr<FileInfo> &file, void *buf,
                  size_t count, off_t offset) -> ssize_t;
auto nextfs_write(const std::shared_ptr<FileInfo> &file, const void *buf,
                  size_t count) -> ssize_t;
auto nextfs_pwrite(const std::shared_ptr<FileInfo> &file, const void *buf,
                   size_t count, off_t offset) -> ssize_t;
auto nextfs_lseek(const std::shared_ptr<FileInfo> &file, off_t offset,
                  int whence) -> off_t;
auto nextfs_unlink(std::string_view pathname) -> int;
} // namespace nextfs
