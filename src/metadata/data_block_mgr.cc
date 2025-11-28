#include "metadata/data_block_mgr.hh"
#include "common/logger.hh"
#include <atomic>
#include <cstdint>

namespace nextfs {

DataBlockManager::DataBlockManager(const std::string &data_block_dir,
                                   uint64_t block_size)
    : block_size_(block_size) {
  for (int i = 0; i < MAX_BLOCK_FILE_NUM; i++) {
    std::string file_path = data_block_dir + "/data_block_" + std::to_string(i);
    fds_[i] = open(file_path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fds_[i] == -1) {
      error("failed to open data block file {}. errno: {} {}", file_path, errno,
            strerror(errno));
    }
    new (&free_lists_[i]) IdxFreeList();
    block_offsets_[i].store(0, std::memory_order_relaxed);
  }
}

DataBlockManager::~DataBlockManager() {
  for (int i = 0; i < MAX_BLOCK_FILE_NUM; i++) {
    if (fds_[i] != -1)
      close(fds_[i]);
  }
}

auto DataBlockManager::alloc_block() -> btable_item {
  uint64_t file_id = next_file_.fetch_add(1, std::memory_order_relaxed);
  file_id = file_id % MAX_BLOCK_FILE_NUM;
  uint64_t offset = free_lists_[file_id].get_one_free_idx();
  if (offset == -1) {
    offset = block_offsets_[file_id].fetch_add(block_size_,
                                               std::memory_order_relaxed);
  }
  btable_item blk = {.file_id_ = file_id, .offset_ = offset};
  return blk;
}

void DataBlockManager::free_block(btable_item blk) {
  free_lists_[blk.file_id_].put_one_free_idx(blk.offset_);
}

} // namespace nextfs