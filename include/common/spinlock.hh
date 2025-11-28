#pragma once
#include <atomic>

#if defined(__x86_64__)

#include <emmintrin.h>
#define PAUSE _mm_pause()

#elif defined(__aarch64__)

#define PAUSE asm volatile("yield" ::: "memory")

#else

#define PAUSE

#endif
namespace nextfs {
class Spinlock {
private:
  std::atomic_uint8_t flag_{0};

public:
  Spinlock(){};
  ~Spinlock() = default;
  auto lock() -> void {
    while (1 == flag_.exchange(1, std::memory_order_acquire)) {
      while (flag_.load(std::memory_order_relaxed) == 1) {
        PAUSE;
      }
    }
  }
  auto unlock() -> void { flag_.store(0, std::memory_order_release); }
  auto try_lock() -> bool {
    return 0 == flag_.exchange(1, std::memory_order_acquire);
  }
  auto is_locked() -> bool {
    return flag_.load(std::memory_order_relaxed) == 1;
  }
};
} // namespace nextfs
