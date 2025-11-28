#pragma once

#include "array"
#include "common/config.hh"
#include <cstdint>
#include <cstring>

namespace nextfs {

// bit0 - read
// bit1 - write
// bit2 - execute
using Permission = uint64_t;
// TODO: if we include daemon/env.hh here, there will me header file cycle
constexpr uint32_t MIN_BLOCK_SIZE = 4 * 1024;
// 4KB size limit
// const uint32_t inode_meta_size = 8 * 5 + 1;
const uint32_t inode_meta_size = 512;
const uint32_t inline_data_size_limit = FILE_INODE_SIZE - inode_meta_size;
// 896, [0 ~ 895]
constexpr uint16_t max_block_count = inline_data_size_limit / 4;

struct ATable {
  // block_0 -> node_A
  // block_1 -> node_B
  // block_2 -> node_C
  std::array<uint32_t, max_block_count> map_{};
};

struct FileInode {
  Permission perm_{7};
  uint64_t create_time_{0};
  uint64_t access_time_{0};
  uint64_t modify_time_{0};
  uint64_t file_size_{0};
  bool has_atable_{false};
  char padding_[inode_meta_size - (8 * 5 + 1)]{};
  // ATable a_table_{};
  char inline_data_[inline_data_size_limit]{0}; // inline data or a table

  FileInode() = default;

  [[nodiscard]] auto cast_ATable() const -> const ATable * {
    return reinterpret_cast<const ATable *>(inline_data_);
  }

  [[nodiscard]] auto cast_ATable() -> ATable * {
    return reinterpret_cast<ATable *>(inline_data_);
  }

  static auto get_offset_perm() -> uint32_t { return 0; }
  static auto get_offset_c_time() -> uint32_t { return sizeof(Permission); }
  static auto get_offset_a_time() -> uint32_t {
    return sizeof(Permission) + sizeof(uint64_t);
  }
  static auto get_offset_m_time() -> uint32_t {
    return sizeof(Permission) + sizeof(uint64_t) * 2;
  }
  static auto get_offset_file_size() -> uint32_t {
    return sizeof(Permission) + sizeof(uint64_t) * 3;
  }
  static auto get_offset_has_atable() -> uint32_t {
    return sizeof(Permission) + sizeof(uint64_t) * 4;
  }
  static auto get_offset_inline_data() -> uint32_t { return inode_meta_size; }
};

// 256B size limlit
struct DirInode {
  Permission perm_{};
  uint64_t create_time_{};
  uint64_t access_time_{};
  uint64_t modify_time_{};
  // etc...
  char padding_[DIR_INODE_SIZE - (8 * 4)]{};
};

// static_assert size
static_assert(sizeof(FileInode) == FILE_INODE_SIZE);
static_assert(sizeof(DirInode) == DIR_INODE_SIZE);

} // namespace nextfs
