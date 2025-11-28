#pragma once
#include "atomic"
#include "limits"
#include "linux/futex.h"
#include "sys/syscall.h"
#include "sys/time.h"
#include "unistd.h"
#include <cstdint>

static inline auto futex(uint32_t *uaddr, int futex_op, uint32_t val,
                         const timespec *timeout = nullptr,
                         uint32_t *uaddr2 = nullptr, uint32_t val3 = 0)
    -> long {
  return syscall(SYS_futex, uaddr, futex_op, val, timeout, uaddr2, val3);
}

static inline auto futex_wait(uint32_t *uaddr, uint32_t val, bool is_private,
                              const timespec *timeout = nullptr) -> long {
  return futex(uaddr, is_private ? FUTEX_WAIT_PRIVATE : FUTEX_WAIT, val,
               timeout);
}

static inline auto futex_wake(uint32_t *uaddr, uint32_t val, bool is_private)
    -> long {
  return futex(uaddr, is_private ? FUTEX_WAKE_PRIVATE : FUTEX_WAKE, val);
}

static constexpr uint32_t PARKED = std::numeric_limits<uint32_t>::max();
static constexpr uint32_t EMPTY = 0;
static constexpr uint32_t NOTIFIED = 1;

class FutexParker {
private:
  bool is_private_;
  std::atomic_uint32_t park_futex_{EMPTY};

public:
  explicit FutexParker(bool is_private) : is_private_(is_private) {}

  void park() {
    if (park_futex_.fetch_sub(1, std::memory_order_acquire) == NOTIFIED) {
      return;
    }
    while (true) {
      uint32_t notified = NOTIFIED;
      futex_wait(reinterpret_cast<uint32_t *>(&park_futex_), PARKED,
                 is_private_, nullptr);
      if (park_futex_.compare_exchange_strong(notified, EMPTY,
                                              std::memory_order_acquire)) {
        return;
      }
      // spurious wake up
    }
  }

  void unpark() {
    if (park_futex_.exchange(NOTIFIED, std::memory_order_release) == PARKED) {
      futex_wake(reinterpret_cast<uint32_t *>(&park_futex_), 1, is_private_);
    }
  }
};

// thread1
// bool complete = false
// send req and &complete to thread2
//
// while(!complete) {
//  park();
// }
// ...

// thread2
// handle req
// *complete = true;
// unpark();
