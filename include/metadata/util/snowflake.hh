#pragma once
#include "assert.h"
#include "common/spinlock.hh"
#include "common/util.hh"
#include "iostream"
#include "mutex"
#include "stdint.h"
#include <atomic>
#include <chrono>
#include <cstdint>

//  96 bits for snowflake
//  |-0-|------ unique id based on start timestamp ++(63 bits) -------|  64 bits
//  |-0-|-------------------- - worker id(32  ) ---------------------|  32 bits
//  16 bits for group id
//  16 bits for server id

namespace nextfs {

class Snowflake {
public:
  Snowflake() = default;
  ~Snowflake() = default;

  static auto init(uint32_t node_id) -> void {
    assert(node_id < max_worker_id_ && node_id > 0);
    worker_id_ = node_id;
    start_ts_ = get_timestamp();

    // last_ts_ = start_ts_;
  }

  static auto instance() -> Snowflake & {
    static Snowflake obj;
    return obj;
  }

  static auto get_timestamp() -> uint64_t {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
  };

  // static auto next_ino() -> ino_type {
  //   std::lock_guard<Spinlock> lock(lock_);
  //   uint64_t ts = get_timestamp();

  //   assert(ts >= last_ts_);

  //   if (ts == last_ts_) {
  //     seq_ = (seq_ + 1) ^ seq_mask_;
  //     // seq overflow
  //     if (seq_ == 0) {
  //       ts = block_wait();
  //     }
  //   } else {
  //     seq_ = 0;
  //   }
  //   last_ts_ = ts;
  //   ino_type ino{
  //       ts - start_ts_,
  //       (worker_id_ << worker_id_shift_) | seq_,
  //       0,
  //       0,
  //   };
  //   return ino;
  // };

  static auto next_ino() -> ino_type {
    uint64_t id = cur_id_.fetch_add(1, std::memory_order_relaxed);
    ino_type ino{
        id + start_ts_,
        worker_id_,
        0,
    };
    return ino;
  };

  // DEPRECATED
  // static auto block_wait() -> uint64_t {
  //   uint64_t ts = get_timestamp();
  //   while (ts <= last_ts_) {
  //     ts = get_timestamp();
  //   }
  //   return ts;
  // };

private:
  // static inline const uint16_t worker_id_bits_ = 10; // bits of worker id
  // static inline const uint16_t seq_bits_ = 21;       // bits of seq num
  static inline const uint64_t max_worker_id_ = -1L ^ (-1L << 32);
  // static inline const uint64_t max_worker_id_ = -1L ^ (-1L <<
  // worker_id_bits_); static inline const uint64_t max_seq_ = -1L ^ (-1L <<
  // seq_bits_); static inline const uint16_t worker_id_shift_ = seq_bits_;
  // static inline const uint16_t ts_shift_ = seq_bits_ + worker_id_bits_;
  // static inline const uint64_t seq_mask_ = -1L ^ (-1L << seq_bits_);

  static inline uint32_t worker_id_;
  // static inline uint32_t seq_;      // not used (DEPRECATED)
  static inline uint64_t start_ts_; // timestamp starting point
  static inline std::atomic_uint64_t cur_id_{0};
  // static inline uint64_t last_ts_; // not used (DEPRECATED)
};

} // namespace nextfs