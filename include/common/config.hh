#pragma once
#include "spdlog/common.h"
#include <cstdint>
#include <optional>
#include <spdlog/spdlog.h>
#include <string>
#include <sys/types.h>
#include <vector>
namespace nextfs {
const char *const CONFIG_ENV = "NEXTFS_CONFIG_PATH";
constexpr uint32_t QUEUE_SIZE = 1024;
constexpr size_t MAX_PATH_LEN = 88;
constexpr size_t FILE_INODE_SIZE = 4096;
constexpr size_t DIR_INODE_SIZE = 256;
// constexpr size_t RDMA_QUEUE_SIZE = 4096;
// constexpr size_t BLOCK_SIZE = 4096;
// constexpr size_t RDMA_BUFFER_SIZE = 16 * BLOCK_SIZE;
// constexpr size_t PAGE_SIZE = 4096;

// struct RemoteConfig {
//   std::vector<std::string> ips_;
//   std::vector<std::string> ports_;

//   std::vector<std::string> rnic_name_;
//   int gid_index_;
// };
struct Config {
  uint16_t group_id_;
  uint16_t server_id_;
  uint32_t node_id_; // group id | server id
  spdlog::level::level_enum log_level_;

  std::string rocksdb_option_path_;

  // node_id-ip-port
  std::vector<std::tuple<uint32_t, std::string, std::string>> in_group_nodes_;

  std::vector<std::tuple<uint32_t, std::string, std::string>>
      out_group_peer_nodes_;
  std::string self_ip_;
  std::string rpc_port_;

  std::string mount_point_;

  uint32_t block_size_;
  uint32_t block_pool_size_;
  std::string block_pool_filename_;
  // std::string ipc_pool_filename_ = "ipc_pool_shm_file";
  // std::string ipc_chnl_filename_ = "ipc_chnl_shm_file";
  int32_t ipc_numa_bind_;
  std::string ipc_pool_filename_;
  std::string ipc_chnl_filename_;
  std::string ipc_futex_filename_;
  uint64_t ipc_pool_size_;
  uint64_t ipc_channel_size_;
  int32_t rpc_numa_bind_;
  uint64_t rpc_mempool_blk_size_;
  uint32_t rpc_recv_head_size_;
  uint64_t rpc_mempool_capacity_;
  uint8_t rpc_shard_num_;
  std::string file_store_policy_;
  std::string dir_store_policy_;
  bool close_to_open_;
  uint64_t dentry_cache_valid_time_ms_;
  // node's id who caches hot spot near root
  uint16_t hot_spot_cache_node_id_;

  std::string dentry_path_;
  std::string metadata_path_;
  std::string btable_path_;
  std::string overflow_atable_path_;
  std::string file_inode_path_;
  std::string dir_inode_path_;
  std::string data_block_dir_;
  uint32_t root_node_id_;
};
auto parse_config(const std::string &config_file) -> Config;
}; // namespace nextfs