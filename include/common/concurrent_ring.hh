#pragma once

#include "common/util.hh"
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace nextfs {

enum EnqueueMode {
  ENQ_MP,
  ENQ_SP,
};

enum DequeueMode {
  DEQ_MC,
  DEQ_SC,
};

template <typename T, EnqueueMode ENQ = ENQ_MP, DequeueMode DEQ = DEQ_MC>
class ConcurrentRing {
private:
  const uint64_t capacity_;
  const uint64_t mask_;

  std::atomic_uint64_t __attribute__((aligned(64))) prod_head_;
  std::atomic_uint64_t __attribute__((aligned(64))) prod_tail_;
  std::atomic_uint64_t __attribute__((aligned(64))) cons_head_;
  std::atomic_uint64_t __attribute__((aligned(64))) cons_tail_;
  T ring_buffer_[1]; // NOLINT
  static constexpr inline auto next_power_of_two(uint64_t val) -> uint64_t {
    return 1 << (64 - __builtin_clzl(val - 1));
  }

public:
  static constexpr auto required_space(size_t capacity) -> size_t {
    return sizeof(ConcurrentRing<T, ENQ, DEQ>) +
           sizeof(T) * (next_power_of_two(capacity) - 1);
  }

  //! caller should call free to ring* after destroy if addr is nullptr
  static auto create_ring(size_t capacity, void *addr = nullptr,
                          bool init = true) -> ConcurrentRing<T, ENQ, DEQ> * {
    if (addr == nullptr) {
      addr = malloc(required_space(capacity));
      if (addr == nullptr) {
        return nullptr;
      }
    }
    if (init) {
      return new (addr)
          ConcurrentRing<T, ENQ, DEQ>(next_power_of_two(capacity));
    } else {
      return reinterpret_cast<ConcurrentRing<T, ENQ, DEQ> *>(addr);
    }
  }

  ~ConcurrentRing() { dequeue_bulk(nullptr, capacity_); }

private:
  ConcurrentRing(size_t capacity)
      : capacity_(capacity), mask_(capacity - 1), prod_head_(0), prod_tail_(0),
        cons_head_(0), cons_tail_(0) {}

public:
  auto size() -> size_t {
    uint64_t cons_tail = cons_tail_.load(std::memory_order_relaxed);
    uint64_t prod_tail = prod_tail_.load(std::memory_order_relaxed);
    uint64_t count = (prod_tail - cons_tail) & mask_;
    return (count > capacity_) ? capacity_ : count;
  }
  auto full() -> bool { return size() == capacity_; }
  auto empty() -> bool { return size() == 0; }
  auto enqueue(const T &obj) -> bool {
    uint64_t prod_head = prod_head_.load(std::memory_order_acquire);
    uint64_t cons_tail;
    bool success = false;
    do {
      cons_tail = cons_tail_.load(std::memory_order_acquire);
      uint64_t free = capacity_ + cons_tail - prod_head;
      if (free == 0) {
        return false;
      }
      if constexpr (ENQ == ENQ_MP) {
        success = prod_head_.compare_exchange_weak(prod_head, prod_head + 1,
                                                   std::memory_order_acq_rel);
      } else {
        prod_head_.store(prod_head + 1, std::memory_order_release);
        success = true;
      }
    } while (unlikely(!success));

    new (&ring_buffer_[prod_head & mask_]) T(obj);

    if constexpr (ENQ == ENQ_MP) {
      while (prod_tail_.load(std::memory_order_relaxed) != prod_head) {
        cpu_relax();
      }
    }
    prod_tail_.store(prod_head + 1, std::memory_order_release);
    return true;
  }

  auto enqueue(T &&obj) -> bool {
    uint64_t prod_head = prod_head_.load(std::memory_order_acquire);
    uint64_t cons_tail;
    bool success = false;
    do {
      cons_tail = cons_tail_.load(std::memory_order_acquire);
      uint64_t free = capacity_ + cons_tail - prod_head;
      if (free == 0) {
        return false;
      }
      if constexpr (ENQ == ENQ_MP) {
        success = prod_head_.compare_exchange_weak(prod_head, prod_head + 1,
                                                   std::memory_order_acq_rel);
      } else {
        prod_head_.store(prod_head + 1, std::memory_order_release);
        success = true;
      }
    } while (unlikely(!success));

    new (&ring_buffer_[prod_head & mask_]) T(std::move(obj));

    if constexpr (ENQ == ENQ_MP) {
      while (prod_tail_.load(std::memory_order_relaxed) != prod_head) {
        cpu_relax();
      }
    }
    prod_tail_.store(prod_head + 1, std::memory_order_release);
    return true;
  }

  auto dequeue(T *obj) -> bool {
    uint64_t cons_head, prod_tail;
    bool success = false;
    cons_head = cons_head_.load(std::memory_order_acquire);

    do {
      prod_tail = prod_tail_.load(std::memory_order_acquire);
      uint64_t used = prod_tail - cons_head;
      if (used == 0) {
        return false;
      }
      if constexpr (DEQ == DEQ_SC) {
        cons_head_.store(cons_head + 1, std::memory_order_release);
        success = true;
      } else {
        success = cons_head_.compare_exchange_weak(cons_head, cons_head + 1,
                                                   std::memory_order_release,
                                                   std::memory_order_acquire);
      }
    } while (unlikely(!success));
    // dequeue

    if (obj != nullptr) {
      new (obj) T(std::move(ring_buffer_[cons_head & mask_]));
    }
    ring_buffer_[cons_head & mask_].~T();

    if constexpr (DEQ == DEQ_MC) {
      while (cons_tail_.load(std::memory_order_relaxed) != cons_head) {
        cpu_relax();
      }
    }
    cons_tail_.store(cons_head + 1, std::memory_order_release);
    return true;
  }

  template <bool use_move>
  auto enqueue_bulk(T *objs, size_t n) -> size_t {
    uint64_t prod_head, cons_tail, count, new_head;
    prod_head = prod_head_.load(std::memory_order_acquire);
    do {
      cons_tail = cons_tail_.load(std::memory_order_acquire);
      uint64_t free = capacity_ + cons_tail - prod_head;
      if (free == 0) {
        return 0;
      }
      count = unlikely(n > free) ? free : n;
      new_head = prod_head + count;
      if constexpr (ENQ == ENQ_SP) {
        prod_head_.store(new_head, std::memory_order_release);
        break;
      } else {
        if (prod_head_.compare_exchange_weak(prod_head, new_head,
                                             std::memory_order_release,
                                             std::memory_order_acquire)) {
          break;
        }
      }
    } while (true);
    // assign
    for (size_t i = 0; i < count; i++) {
      if constexpr (use_move) {
        new (&ring_buffer_[(prod_head + i) & mask_]) T(std::move(objs[i]));
      } else {
        new (&ring_buffer_[(prod_head + i) & mask_])
            T(static_cast<const T &>(objs[i]));
      }
    }

    if constexpr (ENQ == ENQ_MP) {
      while (prod_tail_.load(std::memory_order_relaxed) != prod_head) {
        cpu_relax();
      }
    }
    prod_tail_.store(new_head, std::memory_order_release);
    return count;
  }

  auto dequeue_bulk(T *objs, size_t n) -> size_t {
    uint64_t cons_head, prod_tail, count, new_head;
    cons_head = cons_head_.load(std::memory_order_acquire);
    do {
      prod_tail = prod_tail_.load(std::memory_order_acquire);
      uint64_t used = prod_tail - cons_head;
      if (used == 0) {
        return 0;
      }
      count = unlikely(n > used) ? used : n;
      new_head = cons_head + count;
      if constexpr (DEQ == DEQ_SC) {
        cons_head_.store(new_head, std::memory_order_release);
        break;
      } else {
        if (cons_head_.compare_exchange_weak(cons_head, new_head,
                                             std::memory_order_release,
                                             std::memory_order_acquire)) {
          break;
        }
      }
    } while (true);
    // dequeue
    for (size_t i = 0; i < count; i++) {
      if (objs != nullptr) {
        new (&objs[i]) T(std::move(ring_buffer_[(cons_head + i) & mask_]));
      }
      ring_buffer_[(cons_head + i) & mask_].~T();
    }

    if constexpr (DEQ == DEQ_MC) {
      while (cons_tail_.load(std::memory_order_relaxed) != cons_head) {
        cpu_relax();
      }
    }
    cons_tail_.store(new_head, std::memory_order_release);
    return count;
  }
};

// gyx yyds
// MPMC ring buffer
template <typename T>
class GDRing {
public:
  using value_type = T;
  static constexpr size_t BLOCK_HEADER_SIZE = 2 * CACHE_LINE_SIZE;
  static constexpr size_t MIN_ENTRY_IN_BLOCK = 16;
  static constexpr size_t BLOCK_SIZE =
      align_up(BLOCK_HEADER_SIZE + MIN_ENTRY_IN_BLOCK * sizeof(value_type),
               CACHE_LINE_SIZE);
  static constexpr size_t ENTRY_PER_BLOCK =
      (BLOCK_SIZE - BLOCK_HEADER_SIZE) / sizeof(value_type);

private:
  // const members
  const size_t required_capacity_;
  const size_t BLOCK_NUM = next_power_of_two(
      (required_capacity_ + ENTRY_PER_BLOCK - 1) / ENTRY_PER_BLOCK);
  const size_t MAX_SEG_OFF = BLOCK_NUM;
  const size_t CAPACITY = MAX_SEG_OFF * ENTRY_PER_BLOCK;

private:
  struct Block {
    std::atomic_uint64_t committed_ alignas(CACHE_LINE_SIZE){0};
    std::atomic_uint64_t consumed_ alignas(CACHE_LINE_SIZE){0};
    value_type entries_ alignas(CACHE_LINE_SIZE)[ENTRY_PER_BLOCK];
  };

  struct Segment {
    uint64_t version_;
    uint64_t offset_;
    Segment(uint64_t val) : version_(val >> 32), offset_(val & 0xffffffff) {}
    Segment(uint64_t vsn, uint64_t off) : version_(vsn), offset_(off) {}
    [[nodiscard]] auto to_uint64() const -> uint64_t {
      return (version_ << 32) | offset_;
    }
  };

private:
  std::atomic_uint64_t p_segment_ alignas(CACHE_LINE_SIZE){0};
  std::atomic_uint64_t c_segment_ alignas(CACHE_LINE_SIZE){0};
  Block blocks_[1];

private:
  GDRing(size_t capacity) : required_capacity_(capacity) {
    for (size_t i = 0; i < BLOCK_NUM; i++) {
      blocks_[i].committed_.store(0, std::memory_order_relaxed);
      blocks_[i].consumed_.store(0, std::memory_order_relaxed);
      for (size_t j = 0; j < ENTRY_PER_BLOCK; j++) {
        new (&blocks_[i].entries_[j]) value_type();
      }
    }
  }

public:
  static auto required_space(size_t capacity) -> size_t {
    size_t N =
        next_power_of_two((capacity + ENTRY_PER_BLOCK - 1) / ENTRY_PER_BLOCK);
    return sizeof(GDRing) + (N - 1) * sizeof(Block);
  }

  static auto create_ring(size_t capacity, void *addr = nullptr,
                          bool init = true) -> GDRing * {
    if (addr == nullptr) {
      addr = malloc(required_space(capacity));
      if (addr == nullptr) {
        return nullptr;
      }
    }
    if (init) {
      return new (addr) GDRing(capacity);
    } else {
      return reinterpret_cast<GDRing *>(addr);
    }
  }

  auto enqueue(value_type val) -> bool {
    while (true) {
      Segment p_seg(p_segment_.load(std::memory_order_acquire));
      if (p_seg.offset_ >= MAX_SEG_OFF) {
        if (advance_p_segment(p_seg)) {
          continue;
        } else {
          wait();
          return false;
        }
      }
      p_seg = Segment(p_segment_.fetch_add(1, std::memory_order_release));
      if (p_seg.offset_ >= MAX_SEG_OFF) {
        if (advance_p_segment(p_seg)) {
          continue;
        } else {
          wait();
          return false;
        }
      }
      Block &block = blocks_[p_seg.offset_];
      while (unlikely(block.consumed_.load(std::memory_order_acquire) +
                          ENTRY_PER_BLOCK <=
                      p_seg.version_)) {
        cpu_relax();
      }
      block.entries_[p_seg.version_ % ENTRY_PER_BLOCK] = val;
      while (unlikely(block.committed_.load(std::memory_order_relaxed) !=
                      p_seg.version_)) {
        cpu_relax();
      }
      block.committed_.store(p_seg.version_ + 1, std::memory_order_release);
      return true;
    }
  }

  auto dequeue(value_type *val) -> bool {
    while (true) {
      uint64_t c_seg_raw = c_segment_.load(std::memory_order_acquire);
      Segment c_seg(c_seg_raw);
      if (c_seg.offset_ >= MAX_SEG_OFF) {
        if (advance_c_segment(c_seg)) {
          continue;
        } else {
          return false;
        }
      }
      Block &block = blocks_[c_seg.offset_];
      if (blocks_[BLOCK_NUM - 1].committed_.load(std::memory_order_relaxed) <=
          c_seg.version_) {
        // p c in same line
        if (block.committed_.load(std::memory_order_acquire) <=
            c_seg.version_) {
          return false;
        }
        if (!c_segment_.compare_exchange_strong(c_seg_raw, c_seg_raw + 1,
                                                std::memory_order_acq_rel)) {
          wait();
          continue;
        }
      } else {
        if (!c_segment_.compare_exchange_strong(c_seg_raw, c_seg_raw + 1,
                                                std::memory_order_acq_rel)) {
          continue;
        }
        while (unlikely(block.committed_.load(std::memory_order_acquire) <=
                        c_seg.version_)) {
          cpu_relax();
        }
      }
      if (likely(val != nullptr)) {
        *val = block.entries_[c_seg.version_ % ENTRY_PER_BLOCK];
      }
      while (unlikely(block.consumed_.load(std::memory_order_relaxed) !=
                      c_seg.version_)) {
        cpu_relax();
      }
      block.consumed_.store(c_seg.version_ + 1, std::memory_order_release);
      return true;
    }
  }

  auto dequeue_bulk(value_type *vals, size_t n) -> size_t {
    while (true) {
      uint64_t c_seg_raw = c_segment_.load(std::memory_order_acquire);
      Segment c_seg(c_seg_raw);
      if (c_seg.offset_ >= MAX_SEG_OFF) {
        if (advance_c_segment(c_seg)) {
          continue;
        } else {
          return 0;
        }
      }
      size_t max_n = std::min(n, MAX_SEG_OFF - c_seg.offset_);
      if (blocks_[BLOCK_NUM - 1].committed_.load(std::memory_order_relaxed) <=
          c_seg.version_) {
        // p c in same line
        wait();
        Segment p_seg(p_segment_.load(std::memory_order_acquire));
        if (p_seg.version_ == c_seg.version_) {
          max_n = std::min(max_n, std::min(p_seg.offset_, MAX_SEG_OFF) -
                                      c_seg.offset_);
        }
        // else all entries in this line are allocated
      }
      if (!c_segment_.compare_exchange_strong(c_seg_raw, c_seg_raw + max_n,
                                              std::memory_order_acq_rel)) {
        continue;
      }
      for (size_t i = 0; i < max_n; i++) {
        Block &block = blocks_[c_seg.offset_ + i];
        while (unlikely(block.committed_.load(std::memory_order_acquire) <=
                        c_seg.version_)) {
          cpu_relax();
        }
        if (likely(vals != nullptr)) {
          vals[i] = block.entries_[c_seg.version_ % ENTRY_PER_BLOCK];
        }
        while (unlikely(block.consumed_.load(std::memory_order_relaxed) !=
                        c_seg.version_)) {
          cpu_relax();
        }
        block.consumed_.store(c_seg.version_ + 1, std::memory_order_release);
      }
      return max_n;
    }
  }

private:
  static auto fetch_max(std::atomic_uint64_t &atomic, uint64_t val)
      -> uint64_t {
    uint64_t cur = atomic.load(std::memory_order_relaxed);
    while (unlikely(!atomic.compare_exchange_strong(
        cur, std::max(cur, val), std::memory_order_acq_rel))) {
      if (likely(cur >= val)) {
        break;
      }
    }
    return cur;
  }

  static void wait(uint64_t n = 1024) {
    // asm nop wait
    for (uint64_t i = 0; i < n; i++) {
      asm volatile("nop");
    }
  }

  auto advance_p_segment(Segment seg) -> bool {
    uint64_t c_vsn =
        blocks_[BLOCK_NUM - 1].consumed_.load(std::memory_order_acquire);
    if (seg.version_ >= c_vsn + ENTRY_PER_BLOCK - 1) {
      return false;
    }
    fetch_max(p_segment_, Segment(seg.version_ + 1, 0).to_uint64());
    return true;
  }

  auto advance_c_segment(Segment seg) -> bool {
    uint64_t p_vsn = blocks_[0].committed_.load(std::memory_order_acquire);
    if (seg.version_ + 1 >= p_vsn) {
      return false;
    }
    fetch_max(c_segment_, Segment(seg.version_ + 1, 0).to_uint64());
    return true;
  }
};

} // namespace nextfs