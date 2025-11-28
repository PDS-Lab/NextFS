#pragma once
#include "common/config.hh"
#include "common/logger.hh"
#include "common/spinlock.hh"
#include "common/util.hh"
#include <ankerl/unordered_dense.h>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <sys/types.h>

namespace nextfs {
class BlockInfo {
public:
  void *addr_;
  BlockInfo *next_;
  BlockInfo *prev_;
};
class MemoryPool {
  static constexpr size_t N = 64;

private:
  size_t block_size_;
  size_t block_num_;
  BlockInfo free_lists_[N];
  BlockInfo *nodes_;
  void *buf_{nullptr};
  // lock
  Spinlock locks_[N];
  static constexpr inline auto next_power_of_2(uint64_t val) -> uint64_t {
    return 1 << (64 - __builtin_clzl(val - 1));
  }

public:
  [[nodiscard]] auto addr() -> void * { return buf_; }
  [[nodiscard]] auto blk_size() -> size_t { return block_size_; }
  [[nodiscard]] auto size() -> size_t { return block_size_ * block_num_; }
  // constexpr static auto required_space(size_t block_size, size_t capacity) {
  //   auto blk_size = next_power_of_2(block_size);
  //   auto num = next_power_of_2(capacity / block_size);
  //   return blk_size * num;
  // }
  static auto create_mempool(size_t block_size, size_t block_num,
                             void *addr = nullptr) -> MemoryPool * {
    auto blk_size = next_power_of_2(block_size);
    auto blk_num = next_power_of_2(block_num);
    if (addr == nullptr) {
      addr = malloc(blk_num * blk_size);
      checkp(addr, "failed to malloc for mempool");
    }
    return new MemoryPool(blk_size, blk_num, addr);
  }

private:
  MemoryPool(size_t block_size, size_t block_num, void *addr)
      : block_size_(block_size), block_num_(block_num), buf_(addr) {
    void *ptr = buf_;
    for (auto &free_list : free_lists_) {
      free_list.next_ = &free_list;
      free_list.prev_ = &free_list;
    }
    for (auto &lock : locks_) {
      new (&lock) Spinlock();
    }
    nodes_ = new BlockInfo[block_num_];
    for (int i = 0; i < block_num_; i++) {
      auto *blk = &nodes_[i];
      blk->addr_ = ptr;
      blk->next_ = nullptr;
      blk->prev_ = nullptr;
      size_t shard = (blk - nodes_) % N;
      ptr = reinterpret_cast<void *>((uintptr_t)ptr + block_size_);
      insert_head(blk, &free_lists_[shard]);
    }
  };

public:
  ~MemoryPool() {
    if (buf_ != nullptr)
      free(buf_);
    delete[] nodes_;
  };
  auto get_one_block(int hint = -1) -> BlockInfo * {
    size_t off = hint == -1 ? thread_id : hint;
    off %= N;
    for (int i = 0; i < N; i++) {
      size_t shard = (off + i) % N;
      std::lock_guard<Spinlock> l(locks_[shard]);
      auto dummy = &free_lists_[shard];
      auto info = dummy->prev_;
      if (info == dummy) {
        continue;
      }
      remove_from_list(info);
      return info;
    }
    error("nullptr block");
    return nullptr;
  };
  auto put_one_block(BlockInfo *info) -> void {
    size_t shard = (info - nodes_) % N;
    std::lock_guard<Spinlock> l(locks_[shard]);
    info->next_ = nullptr;
    info->prev_ = nullptr;
    // memset(info->addr_, 0, block_size_);
    insert_head(info, &free_lists_[shard]);
  }

private:
  auto insert_head(BlockInfo *info, BlockInfo *dummy) -> void {
    assert(info->prev_ == nullptr);
    assert(info->next_ == nullptr);
    dummy->next_->prev_ = info;
    info->next_ = dummy->next_;
    info->prev_ = dummy;
    dummy->next_ = info;
  }
  auto remove_from_list(BlockInfo *info) -> void {
    assert(info->prev_ != nullptr);
    assert(info->next_ != nullptr);
    info->prev_->next_ = info->next_;
    info->next_->prev_ = info->prev_;
    info->prev_ = nullptr;
    info->next_ = nullptr;
  }
};

} // namespace nextfs