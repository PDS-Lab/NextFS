#pragma once

#include "common/util.hh"
#include "metadata/util/idx_free_list.hh"
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>

namespace nextfs {

class DataBlockManager {

private:
  static constexpr size_t MAX_BLOCK_FILE_NUM = 64;

private:
  const uint64_t block_size_;
  std::atomic_uint64_t next_file_{0};
  std::array<int, MAX_BLOCK_FILE_NUM> fds_;
  std::array<IdxFreeList, MAX_BLOCK_FILE_NUM> free_lists_;
  std::array<std::atomic_uint64_t, MAX_BLOCK_FILE_NUM> block_offsets_;

public:
  DataBlockManager(const std::string &data_block_dir, uint64_t block_size);
  ~DataBlockManager();
  auto alloc_block() -> btable_item;
  auto free_block(btable_item blk) -> void;
  auto get_fd(uint64_t file_id) -> int { return fds_[file_id]; }
};
} // namespace nextfs