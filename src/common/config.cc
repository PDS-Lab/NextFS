#include "common/config.hh"
#include "common/logger.hh"
#include "common/util.hh"
#include "spdlog/common.h"
#include "yaml-cpp/yaml.h"
#include <cstdint>
#include <fmt/core.h>
#include <optional>
#include <string>
#include <utility>
#include <yaml-cpp/node/node.h>
#include <yaml-cpp/node/parse.h>

namespace nextfs {

auto parse_config(const std::string &config_file) -> Config {
  Config config;
  if (config_file == "") {
    printf("path of config file is null");
  }
  YAML::Node result = YAML::LoadFile(config_file);
  config.log_level_ = static_cast<spdlog::level::level_enum>(
      result["log-level"].as<unsigned short>());

  config.group_id_ = result["group-id"].as<uint16_t>();
  config.server_id_ = result["server-id"].as<uint16_t>();
  config.self_ip_ = result["server-ip"].as<std::string>();
  config.rpc_port_ = result["server-port"].as<std::string>();
  config.node_id_ = get_node_id(config.group_id_, config.server_id_);

  config.mount_point_ = result["mount-point"].as<std::string>();
  config.hot_spot_cache_node_id_ = get_node_id(
      config.group_id_, result["hot-spot-cache-server-id"].as<uint16_t>());

  // in group node
  for (const auto &server : result["in-group-node"]) {
    // auto [ip, id] = v.as<std::pair<std::string, uint16_t>>();
    uint16_t server_id = server["server-id"].as<uint16_t>();
    uint32_t node_id = get_node_id(config.group_id_, server_id);
    std::string ip = server["ip"].as<std::string>();
    std::string port = server["port"].as<std::string>();
    config.in_group_nodes_.push_back(
        std::tuple<uint32_t, std::string, std::string>(node_id, ip, port));
  }

  // out group node
  for (const auto &server : result["out-group-peer-node"]) {
    uint16_t group_id = server["group-id"].as<uint16_t>();
    uint16_t server_id = server["server-id"].as<uint16_t>();
    uint32_t node_id = get_node_id(group_id, server_id);
    std::string ip = server["ip"].as<std::string>();
    std::string port = server["port"].as<std::string>();
    config.out_group_peer_nodes_.push_back(
        std::tuple<uint32_t, std::string, std::string>(node_id, ip, port));
  }

  // // out group node
  // for (const auto &group : result["out-group-peer-node"]) {
  //   uint16_t group_id = group["group"].as<uint16_t>();

  //   for (const auto &server : group) {
  //     uint16_t server_id = server["id"].as<uint16_t>();
  //     std::string ip = server["ip"].as<std::string>();
  //     std::string port = server["port"].as<std::string>();
  //   }
  // }

  config.block_size_ = result["block_size"].as<uint32_t>();
  config.block_pool_size_ = result["block_pool_size"].as<uint32_t>();
  config.block_pool_filename_ = result["block_pool_filename"].as<std::string>();

  config.ipc_numa_bind_ = result["ipc_numa_bind"].as<int32_t>(-1);
  config.ipc_pool_size_ = result["ipc_pool_size"].as<uint64_t>();
  config.ipc_channel_size_ = result["ipc_channel_size"].as<uint64_t>();
  config.rpc_numa_bind_ = result["rpc_numa_bind"].as<int32_t>(-1);
  config.rpc_mempool_blk_size_ = result["rpc_mempool_blk_size"].as<uint64_t>();
  config.rpc_mempool_capacity_ = result["rpc_mempool_capacity"].as<uint64_t>();
  config.rpc_recv_head_size_ = result["rpc_recv_head_size"].as<uint32_t>();
  config.rpc_shard_num_ = result["rpc_shard_num"].as<uint8_t>();

  config.file_store_policy_ = result["file_store_policy"].as<std::string>();
  config.dir_store_policy_ = result["dir_store_policy"].as<std::string>();
  config.close_to_open_ = result["close_to_open"].as<bool>();
  config.dentry_cache_valid_time_ms_ =
      result["dentry_cache_valid_time_ms"].as<uint64_t>(0);

  config.ipc_pool_filename_ = result["ipc_pool_filename"].as<std::string>();
  config.ipc_chnl_filename_ = result["ipc_chnl_filename"].as<std::string>();
  config.dentry_path_ = result["dentry_path"].as<std::string>();
  config.metadata_path_ = result["metadata_path"].as<std::string>();
  config.btable_path_ = result["btable_path"].as<std::string>();
  config.overflow_atable_path_ =
      result["overflow_atable_path"].as<std::string>();
  config.file_inode_path_ = result["file_inode_path"].as<std::string>();
  config.dir_inode_path_ = result["dir_inode_path"].as<std::string>();
  config.data_block_dir_ = result["data_block_dir"].as<std::string>();
  uint16_t root_group_id = result["root_group_id"].as<uint16_t>();
  uint16_t root_server_id = result["root_server_id"].as<uint16_t>();
  config.root_node_id_ = get_node_id(root_group_id, root_server_id);
  config.ipc_pool_filename_ = result["ipc_pool_filename"].as<std::string>();
  config.ipc_chnl_filename_ = result["ipc_chnl_filename"].as<std::string>();
  config.ipc_futex_filename_ = result["ipc_futex_filename"].as<std::string>();

  return config;
}
} // namespace nextfs