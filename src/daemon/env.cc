#include "daemon/env.hh"
#include "common/concurrent_ring.hh"
#include "common/config.hh"
#include "common/consistent_hash.hh"
#include "common/futex.hh"
#include "common/ipc_message.hh"
#include "common/logger.hh"
#include "common/memory_pool.hh"
#include "common/slab.hh"
#include "common/util.hh"
#include "daemon/ipc_receiver.hh"
#include "interceptor/file.hh"
#include "metadata/dentry_db.hh"
#include "metadata/util/snowflake.hh"
#include "rpc/message.hh"
#include "rpc/server.hh"
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <memory>
#include <sys/mman.h>
#include <sys/shm.h>
#include <unistd.h>
#include <vector>

namespace nextfs {
auto GlobalEnv::init() -> int {

  auto &env = instance();
  auto config_path = getenv(nextfs::CONFIG_ENV);

  std::cout << "config_path: " << config_path << std::endl;

  env.parse_config(config_path);
  auto config = env.config_;
  env.node_id_ = config.node_id_;
  GlobalLogger::init(config.log_level_);

  size_t chnl_space =
      ipc_channel_type::required_space(config.ipc_channel_size_);

  int fd = shm_open(config.ipc_chnl_filename_.c_str(), O_CREAT | O_RDWR,
                    S_IRUSR | S_IWUSR);
  if (fd == -1) {
    warn("failed to create ipc channel file, {}", std::strerror(errno));
    return -1;
  }
  int ret = ftruncate(fd, chnl_space);
  check(ret, "ftruncate failed");
  // do we need huge table?
  void *addr =
      mmap(nullptr, chnl_space, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (unlikely(addr == (void *)-1)) {
    shm_unlink(config.ipc_chnl_filename_.c_str());
    warn("failed to mmap addr for ipc channel. {}", std::strerror(errno));
    return -1;
  }
  env.ipc_channel_ =
      ipc_channel_type::create_ring(config.ipc_channel_size_, addr);

  size_t ipc_pool_space = ipc_msg_pool_type::required_space(
      config.ipc_pool_size_, sizeof(IpcMessage));
  fd = shm_open(config.ipc_pool_filename_.c_str(), O_CREAT | O_RDWR,
                S_IRUSR | S_IWUSR);
  if (fd == -1) {
    warn("failed to create ipc pool file, {}", std::strerror(errno));
    return -1;
  }
  ret = ftruncate(fd, ipc_pool_space);
  check(ret, "ftruncate failed");
  // do we need huge table?
  addr =
      mmap(nullptr, ipc_pool_space, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (unlikely(addr == (void *)-1)) {
    shm_unlink(config.ipc_pool_filename_.c_str());
    warn("failed to mmap addr for ipc pool. {}", std::strerror(errno));
    return -1;
  }
  env.ipc_pool_ = ipc_msg_pool_type::create_slab(config.ipc_pool_size_,
                                                 sizeof(IpcMessage), addr);

  fd = shm_open(config.ipc_futex_filename_.c_str(), O_CREAT | O_RDWR,
                S_IRUSR | S_IWUSR);
  if (fd == -1) {
    warn("failed to create ipc futex file, {}", std::strerror(errno));
    return -1;
  }
  ret = ftruncate(fd, sizeof(FutexParker));
  check(ret, "ftruncate for futex failed");
  addr = mmap(nullptr, sizeof(FutexParker), PROT_READ | PROT_WRITE, MAP_SHARED,
              fd, 0);
  if (unlikely(addr == (void *)-1)) {
    shm_unlink(config.ipc_futex_filename_.c_str());
    warn("failed to mmap addr for ipc pool. {}", std::strerror(errno));
    return -1;
  }
  env.ipc_futex_ = new (addr) FutexParker(false);

  fd = shm_open(config.block_pool_filename_.c_str(), O_CREAT | O_RDWR,
                S_IRUSR | S_IWUSR);
  if (fd == -1) {
    warn("failed to create data block pool file, {}", std::strerror(errno));
    return -1;
  }
  size_t data_block_pool_space = block_pool_type::required_space(
      config.block_pool_size_, config.block_size_);
  ret = ftruncate(fd, data_block_pool_space);
  check(ret, "ftruncate for data block pool failed");
  addr = mmap(nullptr, data_block_pool_space, PROT_READ | PROT_WRITE,
              MAP_SHARED, fd, 0);
  if (unlikely(addr == (void *)-1)) {
    shm_unlink(config.block_pool_filename_.c_str());
    warn("failed to mmap addr for data block pool. {}", std::strerror(errno));
    return -1;
  }
  env.block_pool_ = block_pool_type::create_slab(config.block_pool_size_,
                                                 config.block_size_, addr);

  std::vector<uint32_t> in_group_ids;
  in_group_ids.reserve(config.in_group_nodes_.size());
  for (auto v : config.in_group_nodes_) {
    in_group_ids.push_back(std::get<0>(v));
  }

  ConsistentHash::init(std::move(in_group_ids), 31);

  env.ipc_thread_pool_ = std::make_unique<ipc_thread_pool_type>(
      64, 32, ipc_msg_handler, GlobalEnv::config().ipc_numa_bind_);
  for (int i = 0; i < config.rpc_shard_num_; i++) {
    env.rpc_thread_pools_.emplace_back(std::make_unique<rpc_thread_pool_type>(
        64, 8, rpc::handle_recvd_result, GlobalEnv::config().rpc_numa_bind_));
  }

  // RPC
  env.rpc_ = std::make_unique<rpc::RpcServer>(config);

  // Metadata
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;

  env.metadata_handler_ = std::make_unique<MetadataHandler>(
      env.config_.dentry_path_, options, env.config_.metadata_path_, options,
      env.config_.btable_path_, options, env.config_.overflow_atable_path_,
      options);

  return 0;
}

GlobalEnv::~GlobalEnv() {
  stop();
  int ret = 0;
  ret = shm_unlink(instance().config().ipc_chnl_filename_.c_str());
  check(ret, "failed to unlink shm obj--ipc chnl:{}", strerror(errno));
  ret = shm_unlink(instance().config().ipc_pool_filename_.c_str());
  check(ret, "failed to unlink shm obj--ipc pool:{}", strerror(errno));
  ret = shm_unlink(instance().config().ipc_futex_filename_.c_str());
  check(ret, "failed to unlink shm obj--ipc futex:{}", strerror(errno));
  ret = shm_unlink(instance().config().block_pool_filename_.c_str());
  check(ret, "failed to unlink shm obj--data block pool:{}", strerror(errno));
}

} // namespace nextfs