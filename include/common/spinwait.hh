#pragma once
#include "common/util.hh"
#include <cstdint>
namespace nextfs {

class SpinWait {
private:
  static const uint32_t SPIN_THRESHOLD = 128;
  static const uint32_t YILED_THRESHOLD = 8;

private:
  uint32_t counter_{0};
  const uint32_t spin_threshold_;
  const uint32_t yield_threshold_;

public:
  explicit SpinWait(uint32_t spin_threshold = SPIN_THRESHOLD,
                    uint32_t yield_threshold = YILED_THRESHOLD)
      : spin_threshold_(spin_threshold), yield_threshold_(yield_threshold) {}

  template <typename FnYield, typename FnBlock>
  auto spin(const FnYield &yield, const FnBlock &block) -> void {
    if (counter_ < spin_threshold_) {
      cpu_relax();
    } else if (counter_ < (spin_threshold_ + yield_threshold_)) {
      yield();
    } else {
      block();
    }
    counter_++;
  }

  template <typename Fn>
  auto spin_no_block(const Fn &yield) -> void {
    if (counter_ < spin_threshold_) {
      cpu_relax();
    } else {
      yield();
    }
    counter_++;
  }

  auto reset() -> void { counter_ = 0; }
};
} // namespace nextfs
