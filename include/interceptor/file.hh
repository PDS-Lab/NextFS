#pragma once

#include "common/concurrent_ring.hh"
#include "common/slab.hh"
#include "common/spinlock.hh"
#include "common/util.hh"
#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <string_view>
#include <sys/types.h>
#include <utility>
namespace nextfs {
constexpr auto NEXTFS_START_FD = 1050000;

class FileInfo {
private:
  ino_type ino_;
  int fd_;
  uint16_t flags_;
  mode_t mode_;
  std::atomic<off_t> offset_{0};
  std::string file_path_;

public:
  FileInfo(ino_type ino, int fd, int flags, mode_t mode, std::string file_path,
           int offset)
      : ino_(ino), fd_(fd), flags_(flags), mode_(mode),
        file_path_(std::move(file_path)), offset_(offset){};
  ~FileInfo() = default;
  [[nodiscard]] auto ino() const -> ino_type { return ino_; }
  [[nodiscard]] auto fd() const -> int { return fd_; }
  [[nodiscard]] auto flags() const -> int { return flags_; }
  [[nodiscard]] auto mode() const -> mode_t { return mode_; }
  [[nodiscard]] auto file_path_view() const -> std::string_view {
    return std::string_view(file_path_);
  }
  [[nodiscard]] auto file_path() const -> std::string { return file_path_; }
  [[nodiscard]] auto offset() const -> off_t {
    return offset_.load(std::memory_order_acquire);
  }
  auto set_offset(off_t offset) -> void {
    return offset_.store(offset, std::memory_order_release);
  }
};
class OpenFileManager {
private:
  Spinlock lock_;
  Slab<std::shared_ptr<FileInfo>> slab_;

public:
  OpenFileManager() = default;
  ~OpenFileManager() = default;
  auto open(ino_type ino, int flags, mode_t mode, std::string file_path_,
            int offset) -> int {
    std::lock_guard<Spinlock> l(lock_);
    int idx = slab_.insert(std::make_shared<FileInfo>(
        ino, idx + NEXTFS_START_FD, flags, mode, file_path_, offset));
    return idx + NEXTFS_START_FD;
  }
  auto close(int fd) -> int {
    if ((fd < NEXTFS_START_FD)) {
      return -1;
    }
    std::lock_guard<Spinlock> l(lock_);
    if (slab_.get(fd - NEXTFS_START_FD) != nullptr) {
      slab_.remove(fd - NEXTFS_START_FD);
      return 0;
    }
    return -1;
  }
  auto get(int fd) -> std::shared_ptr<FileInfo> {
    if ((fd < NEXTFS_START_FD)) {
      return nullptr;
    }
    std::lock_guard<Spinlock> l(lock_);
    auto ptr = slab_.get(fd - NEXTFS_START_FD);
    return ptr == nullptr ? nullptr : *ptr;
  }
};

// auto resolve_pathname(const char *pathname) -> std::pair<uint8_t,
// std::string>;
} // namespace nextfs
