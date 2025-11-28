#pragma once

#include "ankerl/unordered_dense.h"
#include "common/concurrent_ring.hh"
#include "common/slab.hh"
#include "common/spinlock.hh"
#include "common/util.hh"
#include "metadata/inode.hh"
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string_view>
#include <sys/types.h>
#include <utility>
#include <vector>
namespace nextfs {

class DaemonFileInfo {
  friend class OpenDaemonFileManager;

private:
  mutable std::shared_mutex rwlock_;
  int32_t ref_count_{1};
  // cached blocks that need to update when close
  // only used in close_to_open mode
  std::vector<std::pair<uint32_t, uint32_t>> pending_block_list_;
  FileInode inode_;

public:
  class ReadGuard {
  private:
    const std::shared_ptr<DaemonFileInfo> file_info_;
    bool owns_{true};

  public:
    ReadGuard(std::shared_ptr<DaemonFileInfo> file_info)
        : file_info_(std::move(file_info)) {
      file_info_->rwlock_.lock_shared();
    }
    ~ReadGuard() {
      if (owns_) {
        file_info_->rwlock_.unlock_shared();
      }
    }
    // noncopyable
    ReadGuard(const ReadGuard &) = delete;
    auto operator=(const ReadGuard &) -> ReadGuard & = delete;
    auto operator->() const -> const FileInode * {
      if (!owns_) {
        throw std::runtime_error("ReadGuard has been unlocked");
      }
      return &file_info_->inode_;
    }
    auto unlock() -> void {
      if (!owns_) {
        throw std::runtime_error("ReadGuard has been unlocked");
      }
      file_info_->rwlock_.unlock_shared();
      owns_ = false;
    }
    auto lock() -> void {
      if (owns_) {
        throw std::runtime_error("ReadGuard has been locked");
      }
      file_info_->rwlock_.lock_shared();
      owns_ = true;
    }
    [[nodiscard]] auto pending_blocks() const -> const
        decltype(pending_block_list_) & {
      if (!owns_) {
        throw std::runtime_error("ReadGuard has been unlocked");
      }
      return file_info_->pending_block_list_;
    }
  };
  class WriteGuard {
  private:
    std::shared_ptr<DaemonFileInfo> file_info_;
    bool owns_{true};

  public:
    WriteGuard(std::shared_ptr<DaemonFileInfo> file_info)
        : file_info_(std::move(file_info)) {
      file_info_->rwlock_.lock();
    }
    ~WriteGuard() {
      if (owns_) {
        file_info_->rwlock_.unlock();
      }
    }
    // noncopyable
    WriteGuard(const WriteGuard &) = delete;
    auto operator=(const WriteGuard &) -> WriteGuard & = delete;
    auto operator->() -> FileInode * {
      if (!owns_) {
        throw std::runtime_error("WriteGuard has been unlocked");
      }
      return &file_info_->inode_;
    }
    auto unlock() -> void {
      if (!owns_) {
        throw std::runtime_error("WriteGuard has been unlocked");
      }
      file_info_->rwlock_.unlock();
      owns_ = false;
    }
    auto lock() -> void {
      if (owns_) {
        throw std::runtime_error("WriteGuard has been locked");
      }
      file_info_->rwlock_.lock();
      owns_ = true;
    }
    auto update_inode(const FileInode &inode) -> void {
      if (!owns_) {
        throw std::runtime_error("WriteGuard has been unlocked");
      }
      file_info_->inode_ = inode;
    }
    auto update_pending_blocks(const decltype(pending_block_list_) &block_list)
        -> void {
      if (!owns_) {
        throw std::runtime_error("WriteGuard has been unlocked");
      }
      file_info_->pending_block_list_.insert(
          file_info_->pending_block_list_.end(), block_list.begin(),
          block_list.end());
    }
  };

public:
  DaemonFileInfo() = default;
  explicit DaemonFileInfo(const FileInode &inode) : inode_(inode){};
  ~DaemonFileInfo() = default;
  static auto read(std::shared_ptr<DaemonFileInfo> fi) -> ReadGuard {
    return {std::move(fi)};
  }
  static auto write(std::shared_ptr<DaemonFileInfo> fi) -> WriteGuard {
    return {std::move(fi)};
  }

  auto update_inode(const FileInode &inode) -> void {
    std::lock_guard<std::shared_mutex> l(rwlock_);
    inode_ = inode;
  }
};
class OpenDaemonFileManager {
private:
  static constexpr size_t SHARD_NUM = 32;
  std::array<Spinlock, SHARD_NUM> locks_{};
  std::array<
      ankerl::unordered_dense::map<ino_type, std::shared_ptr<DaemonFileInfo>,
                                   std::hash<ino_type>>,
      SHARD_NUM>
      ino_maps_{};

private:
  auto get_shard(ino_type ino) -> size_t { return ino.ino_high_ % SHARD_NUM; }

public:
  OpenDaemonFileManager() = default;
  ~OpenDaemonFileManager() = default;
  auto open(ino_type ino, const FileInode &inode)
      -> std::shared_ptr<DaemonFileInfo> {
    size_t shard = get_shard(ino);
    std::lock_guard<Spinlock> l(locks_[shard]);
    auto it = ino_maps_[shard].find(ino);
    if (it != ino_maps_[shard].end()) {
      it->second->ref_count_++;
      return it->second;
    } else {
      auto ptr = std::make_shared<DaemonFileInfo>(inode);
      ino_maps_[shard].emplace(ino, ptr);
      return ptr;
    }
  }
  auto open(ino_type ino) -> std::shared_ptr<DaemonFileInfo> {
    size_t shard = get_shard(ino);
    std::lock_guard<Spinlock> l(locks_[shard]);
    auto it = ino_maps_[shard].find(ino);
    if (it != ino_maps_[shard].end()) {
      it->second->ref_count_++;
      return it->second;
    } else {
      auto ptr = std::make_shared<DaemonFileInfo>();
      ino_maps_[shard].emplace(ino, ptr);
      return ptr;
    }
  }
  auto close(ino_type ino) -> bool {
    size_t shard = get_shard(ino);
    std::lock_guard<Spinlock> l(locks_[shard]);
    auto it = ino_maps_[shard].find(ino);
    if (it == ino_maps_[shard].end()) {
      return false;
    }
    it->second->ref_count_--;
    if (it->second->ref_count_ == 0) {
      ino_maps_[shard].erase(it);
      return true;
    }
    return false;
  }
  auto get(ino_type ino) -> std::shared_ptr<DaemonFileInfo> {
    size_t shard = get_shard(ino);
    std::lock_guard<Spinlock> l(locks_[shard]);
    auto it = ino_maps_[shard].find(ino);
    return it == ino_maps_[shard].end() ? nullptr : it->second;
  }
  // auto ergodic_map() -> void {
  //   for (auto &it : ino_map) {
  //     std::cout << it.first.group_id_ << " " << it.first.ino_high_ << " "
  //               << it.first.ino_low_ << " " << it.first.server_id_ <<
  //               std::endl;
  //   }
  // }
};
} // namespace nextfs