#pragma once

#include <ankerl/unordered_dense.h>
#include <cstdint>
#include <fcntl.h>
#include <sys/ipc.h>
#include <syscall.h>
#include <thread>
#include <unistd.h>

using std::string;

#define CACHE_LINE_SIZE 128
#if defined(__x86_64__)
static inline auto cpu_relax() -> void { asm volatile("pause" ::: "memory"); }
#elif defined(__arm64__) || defined(__aarch64__)
static inline auto cpu_relax() -> void { asm volatile("isb"); }
#else
#error "unsupported arch"
#endif

namespace nextfs {

constexpr static auto max_str_len = 80;

static inline auto hash64(uint64_t key) -> uint64_t {
  return ankerl::unordered_dense::detail::wyhash::hash(key);
}
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

static constexpr inline auto next_power_of_two(uint64_t val) -> uint64_t {
  return 1 << (64 - __builtin_clzll(val - 1));
}

static constexpr inline auto align_up(uint64_t val, uint64_t align)
    -> uint64_t {
  return (val + align - 1) & ~(align - 1);
}

static constexpr inline auto align_down(uint64_t val, uint64_t align)
    -> uint64_t {
  return val & ~(align - 1);
}

static constexpr inline auto is_aligned(uint64_t val, uint64_t align) -> bool {
  return (val & (align - 1)) == 0;
}

static constexpr inline auto is_power_of_two(uint64_t val) -> bool {
  return val && !(val & (val - 1));
}

thread_local inline const uint64_t thread_id =
    std::hash<std::thread::id>()(std::this_thread::get_id());

thread_local inline int numa_hint = -1;

static inline auto get_cpu_numa(uint32_t *cpu_id, uint32_t *numa_id) -> void {
  int ret = syscall(SYS_getcpu, cpu_id, numa_id);
  // int ret = getcpu(&cpu_id, &numa_id);
  if (unlikely(ret != 0)) {
    throw std::runtime_error("failed to get numa id");
  }
}

static inline auto self_cpu_id() -> uint32_t {
  uint32_t cpu_id;
  get_cpu_numa(&cpu_id, nullptr);
  return cpu_id;
}

static inline auto self_numa_id() -> uint32_t {
  if (numa_hint != -1) {
    return numa_hint;
  }
  uint32_t numa_id;
  get_cpu_numa(nullptr, &numa_id);
  return numa_id;
}

// uint32_t node_id = group_id_ << INO_GROUP_ID_SHIFT | server_id_
constexpr auto INO_GROUP_ID_SHIFT = 16;
constexpr auto INO_SERVER_ID_MASK = 0xffff;

static constexpr inline auto get_node_id(uint16_t group_id, uint16_t server_id)
    -> uint32_t {
  return group_id << INO_GROUP_ID_SHIFT | (uint32_t)server_id;
}

static constexpr inline auto get_group_id(uint32_t node_id) -> uint16_t {
  return node_id >> INO_GROUP_ID_SHIFT;
}

static constexpr inline auto get_server_id(uint32_t node_id) -> uint16_t {
  return node_id & INO_SERVER_ID_MASK;
}

inline auto get_timestamp() -> uint64_t {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
};

// 16B
struct ino_type {
  uint64_t ino_high_{};
  uint32_t ino_low_{};
  uint16_t group_id_{0};
  uint16_t server_id_{0};
  [[nodiscard]] auto server_id() const -> uint16_t { return server_id_; }
  [[nodiscard]] auto group_id() const -> uint16_t { return group_id_; }
  [[nodiscard]] auto node_id() const -> uint32_t {
    return group_id_ << INO_GROUP_ID_SHIFT | server_id_;
  }
  [[nodiscard]] auto hash() const -> uint64_t {
    return hash64(ino_high_ ^
                  (uint64_t(ino_low_) << 32 | uint64_t(group_id_) << 16 |
                   uint64_t(server_id_)));
  }
  auto operator<(ino_type const &_A) const -> bool {
    if (ino_high_ < _A.ino_high_)
      return true;
    if (ino_high_ > _A.ino_high_)
      return false;

    if (ino_low_ < _A.ino_low_)
      return true;
    if (ino_low_ > _A.ino_low_)
      return false;

    if (group_id_ < _A.group_id_)
      return true;
    if (group_id_ > _A.group_id_)
      return false;

    if (server_id_ < _A.server_id_)
      return true;
    if (server_id_ > _A.server_id_)
      return false;
    return false;
  }
  // write equal function for ino_type
  auto operator==(ino_type const &_A) const -> bool {
    if (ino_high_ == _A.ino_high_ && ino_low_ == _A.ino_low_ &&
        group_id_ == _A.group_id_ && server_id_ == _A.server_id_)
      return true;
    return false;
  }
};

enum class Type : uint16_t {
  File = 0,
  Directory,
};

struct ino_idx {
  Type type_;
  uint64_t offset_; // offset of the inode block in ext4 file
};

struct dentry_key {
  ino_type ino_;
  char subdir_name_[max_str_len];
};

struct dentry {
  uint32_t sub_group_;
  uint32_t sub_id_;
  ino_type sub_ino_;
};

// the key of B table is btable_key
// the value of B table is btable_block_item
struct btable_key {
  ino_type ino_;
  uint32_t block_num_;
};

struct btable_item {
  uint64_t file_id_ : 8;
  uint64_t offset_ : 56;
};

struct overflow_atable_key {
  ino_type ino_;
  uint32_t block_num_;
};

struct overflow_atable_item {
  uint32_t node_id_;
};

} // namespace nextfs

namespace std {
template <>
struct hash<nextfs::ino_type> {
  using is_avalanching = void;
  auto operator()(const nextfs::ino_type &ino) const -> size_t {
    return ino.hash();
  }
};
} // namespace std