#pragma once

#include "common/concurrent_ring.hh"
#include "common/config.hh"
#include "common/futex.hh"
#include "common/ipc_message.hh"
#include "common/memory_pool.hh"
#include "common/slab.hh"
#include "common/thread_pool.hh"
#include "daemon_file.hh"
#include "metadata/metadata.hh"
#include "rpc/handler.hh"
#include "rpc/server.hh"
#include <cstdint>
#include <mutex>
#include <rocksdb/db.h>
#include <string>
#include <vector>

namespace nextfs {

class MetadataHandler;

class GlobalEnv {
  using ipc_channel_type = GDRing<IpcMessageInfo>;
  // using ipc_channel_type = ConcurrentRing<IpcMessageInfo, ENQ_MP, DEQ_SC>;
  using ipc_msg_pool_type = StaticSlab;
  using ipc_thread_pool_type = v2::ThreadPool<IpcMessageInfo>;
  using rpc_thread_pool_type = v2::ThreadPool<rpc::RpcHandleArg>;
  using block_pool_type = StaticSlab;

private:
  Config config_;
  OpenDaemonFileManager open_daemon_file_manager_;
  uint32_t node_id_;
  int ipc_channel_shm_id_{-1};
  int ipc_obj_pool_shm_id_{-1};
  int data_block_pool_shm_id_{-1};
  std::unique_ptr<ipc_thread_pool_type> ipc_thread_pool_;
  std::vector<std::unique_ptr<rpc_thread_pool_type>> rpc_thread_pools_;
  std::atomic_bool stop_{false};
  std::unique_ptr<rpc::RpcServer> rpc_;
  ipc_channel_type *ipc_channel_{nullptr};
  ipc_msg_pool_type *ipc_pool_{nullptr};
  FutexParker *ipc_futex_{nullptr};
  block_pool_type *block_pool_{nullptr};
  std::unique_ptr<MetadataHandler> metadata_handler_{nullptr};
  // public:
  //   // Metadata related
  //   uint32_t root_node_id_{};
  //   ino_type root_ino_{};
  //   std::string file_inode_path_{};
  //   std::string dir_inode_path_{};
  //   std::string data_block_path_{};
  //   uint32_t file_inode_size_{4 * 1024}; // bytes
  //   uint32_t dir_inode_size_{256};
  //   uint32_t block_size_{};

public:
  ~GlobalEnv();
  static auto init() -> int;
  static auto instance() -> GlobalEnv & {
    static GlobalEnv env;
    return env;
  }

  static auto parse_config(const std::string &config_file) -> void {
    instance().config_ = std::move(nextfs::parse_config(config_file));
  };
  static auto stop() -> void {
    instance().stop_ = true;
    GlobalEnv::ipc_futex()->unpark();
    instance().ipc_thread_pool_.reset();
    instance().rpc_thread_pools_.clear();
    instance().rpc_.reset();
    instance().metadata_handler_.reset();
  }
  [[nodiscard]] static auto is_stopped() -> bool { return instance().stop_; }
  [[nodiscard]] static auto open_daemon_file_manager()
      -> OpenDaemonFileManager & {
    return instance().open_daemon_file_manager_;
  }
  [[nodiscard]] static auto node_id() -> uint32_t {
    return instance().node_id_;
  }
  [[nodiscard]] static auto group_id() -> uint16_t {
    return instance().node_id_ >> INO_GROUP_ID_SHIFT;
  }
  [[nodiscard]] static auto server_id() -> uint16_t {
    return instance().node_id_ & INO_SERVER_ID_MASK;
  }
  [[nodiscard]] static auto config() -> const Config & {
    return instance().config_;
  }
  [[nodiscard]] static auto rpc() -> rpc::RpcServer * {
    return instance().rpc_.get();
  }
  [[nodiscard]] static auto block_pool() -> block_pool_type * {
    return instance().block_pool_;
  }
  [[nodiscard]] static auto ipc_channel() -> ipc_channel_type * {
    return instance().ipc_channel_;
  }
  [[nodiscard]] static auto ipc_pool() -> ipc_msg_pool_type * {
    return instance().ipc_pool_;
  }
  [[nodiscard]] static auto ipc_futex() -> FutexParker * {
    return instance().ipc_futex_;
  }
  [[nodiscard]] static auto mount_point() -> std::string & {
    return instance().config_.mount_point_;
  }
  [[nodiscard]] static auto hot_spot_cache_node_id() -> uint16_t {
    return instance().config_.hot_spot_cache_node_id_;
  }
  [[nodiscard]] static auto ipc_thread_pool() -> ipc_thread_pool_type * {
    return instance().ipc_thread_pool_.get();
  }
  [[nodiscard]] static auto rpc_thread_pool(int shard)
      -> rpc_thread_pool_type * {
    return instance().rpc_thread_pools_[shard].get();
  }
  [[nodiscard]] static auto metadata_handler() -> MetadataHandler * {
    return instance().metadata_handler_.get();
  }
};
} // namespace nextfs