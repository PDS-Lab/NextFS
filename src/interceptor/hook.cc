#include "interceptor/hook.hh"
#include "common/logger.hh"
#include "interceptor/nextfs.hh"
#include "interceptor/preload.hh"
#include "interceptor/util.hh"
#include "spdlog/fmt/bundled/format.h"
#include <cerrno>
#include <fcntl.h>
#include <libsyscall_intercept_hook_point.h>
#include <sys/syscall.h>
namespace nextfs {

static inline auto with_errno(int64_t ret) -> int64_t {
  return (ret < 0) ? -errno : ret;
}

auto hook_wrapper(long syscall_num, long a0, long a1, long a2, long a3, long a4,
                  long a5, long *res) -> int {
  // //trace("hook!");
  thread_local bool reentrance_guard_flag = false;
  int hooked, o_errno;
  if (reentrance_guard_flag)
    return 1;
  reentrance_guard_flag = true;
  o_errno = errno;
  hooked = hook(syscall_num, a0, a1, a2, a3, a4, a5, res);
  errno = o_errno;
  reentrance_guard_flag = false;
  return hooked;
}

auto hook(long syscall_num, long a0, long a1, long a2, long a3, long a4,
          long a5, long *res) -> int {
  switch (syscall_num) {
  case SYS_mkdir:
    // a0 pathname; a1 mode
    *res = hook_mkdirat(AT_FDCWD, reinterpret_cast<const char *>(a0), a1);
    break;
  case SYS_mkdirat:
    // a0 dirfd; a1 pathname; a2 mode
    *res = hook_mkdirat(static_cast<int>(a0),
                        reinterpret_cast<const char *>(a1), a2);
    break;
  case SYS_open:
    // a0 pathname; a1 flags; a2 mode
    *res = hook_openat(AT_FDCWD, reinterpret_cast<const char *>(a0), a1, a2);
    break;
  case SYS_openat:
    // a0 dirfd; a1 pathname; a2 flags; a3 mode
    *res = hook_openat(static_cast<int>(a0), reinterpret_cast<const char *>(a1),
                       a2, a3);
    break;
  case SYS_creat:
    // a0 pathname; a1 mode
    *res = hook_openat(AT_FDCWD, reinterpret_cast<const char *>(a0),
                       O_CREAT | O_WRONLY | O_TRUNC, a1);
    break;
  case SYS_close:
    // a0 fd
    *res = hook_close(static_cast<int>(a0));
    break;
  case SYS_stat:
    // a0 pathname; a1 statbuf
    *res = hook_stat(reinterpret_cast<const char *>(a0),
                     reinterpret_cast<struct stat *>(a1));
    break;
  case SYS_lstat:
    // a0 pathname; a1 statbuf
    *res = hook_stat(reinterpret_cast<const char *>(a0),
                     reinterpret_cast<struct stat *>(a1));
    break;
  case SYS_fstat:
    // a0 fd; a1 statbuf
    *res = hook_fstat(a0, reinterpret_cast<struct stat *>(a1));
    break;
  case SYS_newfstatat:
    // a0 dirfd; a1 pathname; a2 statbuf; a3 flags
    if (a3 & AT_EMPTY_PATH) {
      *res = hook_fstat(a0, reinterpret_cast<struct stat *>(a2));
    } else if (a0 & AT_FDCWD) {
      *res = hook_stat(reinterpret_cast<const char *>(a1),
                       reinterpret_cast<struct stat *>(a2));
    } else {
      return 1;
    }
    break;
  case SYS_fsync:
    // a0 fd
    *res = hook_fsync(a0);
    break;
  case SYS_read:
    // a0 fd; a1 buf; a2 count
    *res = hook_read(static_cast<int>(a0), reinterpret_cast<void *>(a1),
                     static_cast<size_t>(a2));
    break;
  case SYS_pread64:
    // a0 fd; a1 buf; a2 count; a3 offset
    *res = hook_pread(static_cast<int>(a0), reinterpret_cast<void *>(a1),
                      static_cast<size_t>(a2), static_cast<off_t>(a3));
    break;
  case SYS_write:
    // a0 fd; a1 buf; a2 count
    *res = hook_write(static_cast<int>(a0), reinterpret_cast<const void *>(a1),
                      static_cast<size_t>(a2));
    break;
  case SYS_pwrite64:
    // a0 fd; a1 buf; a2 count; a3 offset
    *res = hook_pwrite(static_cast<int>(a0), reinterpret_cast<const void *>(a1),
                       static_cast<size_t>(a2), static_cast<off_t>(a3));
    break;
  case SYS_unlink:
    // a0 pathname
    *res = hook_unlink(reinterpret_cast<char *>(a0));
    break;
  case SYS_rmdir:
    // a0 pathname
    *res = hook_unlink(reinterpret_cast<char *>(a0));
    break;
  case SYS_lseek:
    // a0 fd; a1 offset; a2 whence
    *res = hook_lseek(static_cast<int>(a0), static_cast<off_t>(a1),
                      static_cast<int>(a2));
    break;
  case SYS_rename:
    // a0 oldpath; a1 newpath
    *res = hook_rename(reinterpret_cast<const char *>(a0),
                       reinterpret_cast<const char *>(a1));
    break;
  default:
    // trace("syscall {} not hooked", (int)syscall_num);
    return 1;
  }
  return 0;
}

// mkdir success return 0; fail return 1;
auto hook_mkdirat(int dirfd, const char *pathname, mode_t mode) -> int {
  if (dirfd != AT_FDCWD) {
    // PreloadContext::logger()->warn("dirfd not AT_FDCWD, not supported");
    return syscall_no_intercept(SYS_mkdirat, dirfd, pathname, mode);
  }
  if (pathname == nullptr) {
    warn("pathname is nullptr");
    return syscall_no_intercept(SYS_mkdirat, dirfd, pathname, mode);
  }
  auto real_path = resolve_path(PreloadContext::cwd(), pathname);
  auto &mount_point = PreloadContext::mount_point();

  // info("real_path : {}", real_path);
  // info("mount_point : {}", mount_point);

  if (!real_path.starts_with(mount_point)) {
    info("mkdir not in the mount point");
    return syscall_no_intercept(SYS_mkdirat, dirfd, pathname, mode);
  }

  trace("hook mkdirat({}, {}, {:#o})", dirfd, pathname, mode);
  return with_errno(nextfs_mkdirat(
      dirfd, std::string_view(real_path).substr(mount_point.size()), mode));
}

// open success return fd; fail return 1;
auto hook_openat(int dirfd, const char *pathname, int flags, mode_t mode)
    -> int {
  if (dirfd != AT_FDCWD) {
    // PreloadContext::logger()->warn("dirfd not AT_FDCWD, not supported");
    return syscall_no_intercept(SYS_openat, dirfd, pathname, flags, mode);
  }
  if (pathname == nullptr) {
    warn("pathname is nullptr");
    return syscall_no_intercept(SYS_openat, dirfd, pathname, flags, mode);
  }
  auto real_path = resolve_path(PreloadContext::cwd(), pathname);
  auto &mount_point = PreloadContext::mount_point();

  if (!real_path.starts_with(mount_point)) {
    return syscall_no_intercept(SYS_openat, dirfd, pathname, flags, mode);
  }

  trace("hook openat({}, {}, {:#o}, {:#o})", dirfd, pathname, flags, mode);
  return with_errno(nextfs_openat(
      std::string_view(real_path).substr(mount_point.size()), flags, mode));
}

// close success return 0; fail return 1;
auto hook_close(int fd) -> int {
  if (auto fi = PreloadContext::open_file_manager().get(fd); fi != nullptr) {
    trace("hook close({})", fd);
    return with_errno(nextfs_close(fi));
  }
  return syscall_no_intercept(SYS_close, fd);
}

auto hook_stat(const char *pathname, struct stat *buf) -> int {
  // //info("hook stat({}, {}, {})", pathname, statbuf, flags);
  // return nextfs_stat(pathname, statbuf, flags);
  if (pathname == nullptr) {
    warn("pathname is nullptr");
    return syscall_no_intercept(SYS_stat, pathname, buf);
  }
  if (buf == nullptr) {
    warn("buf is nullptr");
    return syscall_no_intercept(SYS_stat, pathname, buf);
  }
  auto real_path = resolve_path(PreloadContext::cwd(), pathname);
  auto &mount_point = PreloadContext::mount_point();

  if (!real_path.starts_with(mount_point)) {
    return syscall_no_intercept(SYS_stat, pathname, buf);
  }

  trace("hook stat({}, {})", pathname, fmt::ptr(buf));
  return with_errno(
      nextfs_stat(std::string_view(real_path).substr(mount_point.size()), buf));
}

auto hook_fstat(int fd, struct stat *buf) -> int {
  if (auto fi = PreloadContext::open_file_manager().get(fd); fi != nullptr) {
    trace("hook fstat({}, {})", fd, fmt::ptr(buf));
    return with_errno(nextfs_fstat(fi, buf));
  }
  return syscall_no_intercept(SYS_fstat, fd, buf);
}

auto hook_fsync_(int fd) -> int {
  if (auto fi = PreloadContext::open_file_manager().get(fd); fi != nullptr) {
    // info("hook fsync({})", fd);
    //  TODO: write fsync
    return 0;
  }
  // warn("hook fsync fi==nullptr");
  return syscall_no_intercept(SYS_stat, fd);
}

auto hook_read(int fd, void *buf, size_t count) -> ssize_t {
  if (auto fi = PreloadContext::open_file_manager().get(fd); fi != nullptr) {
    trace("hook read({}, {}, {})", fd, buf, count);
    return with_errno(nextfs_read(fi, buf, count));
  }
  return syscall_no_intercept(SYS_read, fd, buf, count);
}

auto hook_pread(int fd, void *buf, size_t count, off_t offset) -> ssize_t {
  if (auto fi = PreloadContext::open_file_manager().get(fd); fi != nullptr) {
    trace("hook pread({}, {}, {}, {})", fd, buf, count, offset);
    return with_errno(nextfs_pread(fi, buf, count, offset));
  }
  return syscall_no_intercept(SYS_read, fd, buf, count);
}

auto hook_write(int fd, const void *buf, size_t count) -> ssize_t {
  if (auto fi = PreloadContext::open_file_manager().get(fd); fi != nullptr) {
    trace("hook write({}, {}, {})", fd, buf, count);
    return with_errno(nextfs_write(fi, buf, count));
  }
  return syscall_no_intercept(SYS_write, fd, buf, count);
}

auto hook_pwrite(int fd, const void *buf, size_t count, off_t offset)
    -> ssize_t {
  if (auto fi = PreloadContext::open_file_manager().get(fd); fi != nullptr) {
    trace("hook pwrite({}, {}, {}, {})", fd, buf, count, offset);
    return with_errno(nextfs_pwrite(fi, buf, count, offset));
  }
  return syscall_no_intercept(SYS_write, fd, buf, count);
}

auto hook_unlink(const char *pathname) -> int {
  trace("hook unlink({})", pathname);
  if (pathname == nullptr) {
    warn("pathname is nullptr");
    return syscall_no_intercept(SYS_unlink, pathname);
  }
  auto real_path = resolve_path(PreloadContext::cwd(), pathname);
  auto &mount_point = PreloadContext::mount_point();
  if (!real_path.starts_with(mount_point)) {
    return syscall_no_intercept(SYS_unlink, pathname);
  }
  return with_errno(
      nextfs_unlink(std::string_view(real_path).substr(mount_point.size())));
}

auto hook_lseek(int fd, off_t offset, int whence) -> int {
  if (auto fi = PreloadContext::open_file_manager().get(fd); fi != nullptr) {
    trace("hook lseek({}, {}, {})", fd, offset, whence);
    return with_errno(nextfs_lseek(fi, offset, whence));
  }
  return syscall_no_intercept(SYS_lseek, fd, offset, whence);
}

auto hook_rename(const char *oldpath, const char *newpath) -> int {
  if (oldpath == nullptr || newpath == nullptr) {
    return syscall_no_intercept(SYS_rename, oldpath, newpath);
  }
  auto old_real_path = resolve_path(PreloadContext::cwd(), oldpath);
  auto new_real_path = resolve_path(PreloadContext::cwd(), newpath);
  auto &mount_point = PreloadContext::mount_point();
  bool old_is_mounted = old_real_path.starts_with(mount_point);
  bool new_is_mounted = new_real_path.starts_with(mount_point);
  if (!old_is_mounted && !new_is_mounted) {
    return syscall_no_intercept(SYS_rename, oldpath, newpath);
  } else if (old_is_mounted != new_is_mounted) {
    return -EXDEV;
  }
  trace("hook renameat({}, {})", oldpath, newpath);
  return with_errno(nextfs_rename(
      std::string_view(old_real_path).substr(mount_point.size()),
      std::string_view(new_real_path).substr(mount_point.size())));
}
} // namespace nextfs
