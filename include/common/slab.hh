#pragma once

#include "common/spinlock.hh"
#include "util.hh"
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <mutex>
#include <new>
#include <optional>
#include <sys/mman.h>
#include <sys/types.h>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>
namespace nextfs {

template <typename T>
class Slab {
private:
  struct Slot {
    std::variant<std::monostate, uint32_t, T> data_;
  };
  constexpr static size_t SLOT_SIZE = sizeof(Slot);
  constexpr static size_t RAW_NUM_SLOTS = 4096 / SLOT_SIZE;
  constexpr static size_t NUM_SLOTS = 1 << (31 - __builtin_clz(RAW_NUM_SLOTS));
  constexpr static size_t PAGE_SHIFT =
      32 - __builtin_clz(NUM_SLOTS - 1);              // high bits number
  constexpr static size_t INDEX_MASK = NUM_SLOTS - 1; // low bits

  uint32_t next_{0};
  std::vector<Slot *> page_;

public:
  Slab() = default;
  ~Slab() {
    for (Slot *ptr : page_) {
      for (size_t i = 0; i < NUM_SLOTS; i++) {
        ptr[i].~Slot();
      }
      free(ptr);
    }
  }
  Slab(Slab &&other) noexcept {
    next_ = std::exchange(other.next_, 0);
    page_.swap(other.page_);
  }
  auto operator=(Slab &&other) noexcept -> Slab & {
    if (this != &other) {
      next_ = std::exchange(other.next_, 0);
      page_.swap(other.page_);
    }
    return *this;
  }
  auto insert(const T &obj) -> uint32_t {
    size_t page_idx = next_ >> PAGE_SHIFT;
    size_t slot_idx = next_ & INDEX_MASK;
    if (page_idx >= page_.size()) {
      alloc_page();
    }
    Slot *p = page_[page_idx];
    auto next_idx = std::get<uint32_t>(p[slot_idx].data_);
    p[slot_idx].data_ = obj;
    return std::exchange(next_, next_idx);
  }
  auto insert(T &&obj) -> uint32_t {
    size_t page_idx = next_ >> PAGE_SHIFT;
    size_t slot_idx = next_ & INDEX_MASK;
    if (page_idx >= page_.size()) {
      alloc_page();
    }
    Slot *p = page_[page_idx];
    auto next_idx = std::get<uint32_t>(p[slot_idx].data_);
    p[slot_idx].data_ = std::move(obj);
    return std::exchange(next_, next_idx);
  }
  auto remove(uint32_t idx) -> T {
    size_t page_idx = idx >> PAGE_SHIFT;
    size_t slot_idx = idx & INDEX_MASK;
    Slot *p = page_[page_idx];
    T tmp = std::get<T>(std::move(p[slot_idx].data_));
    p[slot_idx].data_ = next_; // like linked list
    next_ = idx;
    return tmp;
  }
  auto get(uint32_t idx) -> T * {
    size_t page_idx = idx >> PAGE_SHIFT;
    size_t slot_idx = idx & INDEX_MASK;
    if (page_idx >= page_.size() || slot_idx > NUM_SLOTS) {
      return nullptr;
    }
    auto &p = page_[page_idx];
    if (!std::holds_alternative<T>(p[slot_idx].data_))
      return nullptr;
    return &std::get<T>(p[slot_idx].data_);
  }

private:
  auto alloc_page() -> void {
    uint32_t next_page_id = page_.size() << PAGE_SHIFT;
    Slot *ptr = nullptr;
    if (0 != posix_memalign(reinterpret_cast<void **>(&ptr), 4096,
                            NUM_SLOTS * SLOT_SIZE)) {
      throw std::bad_alloc();
    }
    page_.push_back(ptr);
    for (size_t i = 0; i < NUM_SLOTS; i++) {
      new (&ptr[i]) Slot;
      if (i == NUM_SLOTS - 1) {
        ptr[i].data_ = static_cast<uint32_t>(page_.size() << PAGE_SHIFT);
      } else {
        ptr[i].data_ = static_cast<uint32_t>(next_page_id | (i + 1));
      }
    }
  }
};

class StaticSlab {
private:
  static constexpr size_t SHARD_NUM = 64;
  static constexpr size_t SHARD_BITS = __builtin_ctzll(SHARD_NUM);
  static constexpr size_t SHARD_MASK = SHARD_NUM - 1;
  const uint64_t capacity_;
  const uint64_t slot_size_;
  const uint64_t shard_size_ = capacity_ / SHARD_NUM;
  struct alignas(CACHE_LINE_SIZE) Shard {
    Spinlock lock_{};
    uint64_t next_{0};
  };
  Shard shards_[SHARD_NUM];
  uint8_t pool_ alignas(4096)[1];

public:
  static constexpr auto required_space(size_t capacity, size_t slot_size)
      -> size_t {
    assert(capacity < std::numeric_limits<uint32_t>::max());
    return offsetof(StaticSlab, pool_) +
           capacity * std::max(slot_size, sizeof(uint64_t));
  }

  //! caller should call free to pool* after destroy if addr is nullptr
  static auto create_slab(size_t capacity, size_t slot_size,
                          void *addr = nullptr, bool init = true)
      -> StaticSlab * {
    assert(capacity < std::numeric_limits<uint32_t>::max());
    if (addr == nullptr) {
      addr = malloc(required_space(capacity, slot_size));
      if (addr == nullptr) {
        return nullptr;
      }
    }
    if (init) {
      return new (addr) StaticSlab(capacity, slot_size);
    } else {
      return reinterpret_cast<StaticSlab *>(addr);
    }
  }

public:
  StaticSlab(size_t capacity, size_t slot_size)
      : capacity_(capacity), slot_size_(std::max(slot_size, sizeof(uint64_t))) {
    assert(capacity < std::numeric_limits<uint32_t>::max());
    for (uint64_t i = 0; i < SHARD_NUM; ++i) {
      new (&shards_[i]) Shard();
      for (uint64_t j = 0; j < shard_size_; j++) {
        *cast_next(at(j << SHARD_BITS | i)) = j + 1;
      }
    }
  }

  auto addr() -> void * {
    return (void *)(reinterpret_cast<uintptr_t>(this) +
                    offsetof(StaticSlab, pool_));
  }
  auto size() -> size_t {
    return capacity_ * std::max(slot_size_, sizeof(uint64_t));
  }

  template <typename T>
  auto alloc(ssize_t shard_hint = -1)
      -> std::optional<std::pair<uint32_t, T *>> {
    assert(sizeof(std::conditional<std::is_void_v<T>, uint32_t, T>) <=
           slot_size_);
    size_t shard = shard_hint == -1 ? pick_shard() : shard_hint % SHARD_NUM;
    for (int i = 0; i < SHARD_NUM - 1; i++) {
      std::lock_guard<Spinlock> l(shards_[shard].lock_);
      uint64_t next = shards_[shard].next_;
      if (next == shard_size_) {
        shard = (shard + 1) % SHARD_NUM;
        continue;
      }
      shards_[shard].next_ = *cast_next(at(next << SHARD_BITS | shard));
      return std::make_pair(
          next << SHARD_BITS | shard,
          reinterpret_cast<T *>(at(next << SHARD_BITS | shard)));
    }
    return std::nullopt;
  }

  auto free(uint32_t index) -> void {
    assert(index < capacity_);
    size_t shard = index & SHARD_MASK;
    uint64_t new_next = index >> SHARD_BITS;
    std::lock_guard<Spinlock> l(shards_[shard].lock_);
    uint64_t next = shards_[shard].next_;
    *cast_next(at(index)) = next;
    shards_[shard].next_ = new_next;
  }

  template <typename T>
  auto get(uint32_t index) -> T * {
    assert(index < capacity_ &&
           sizeof(std::conditional<std::is_void_v<T>, uint32_t, T>) <=
               slot_size_);
    return reinterpret_cast<T *>(at(index));
  }

private:
  auto at(uint64_t index) -> void * {
    return pool_ + uint64_t(index) * uint64_t(slot_size_);
  }
  auto cast_next(void *addr) -> uint64_t * {
    return reinterpret_cast<uint64_t *>(addr);
  }
  auto pick_shard() -> size_t { return thread_id & SHARD_MASK; }
};
} // namespace nextfs