#pragma once
#include "common/concurrent_ring.hh"
#include "common/config.hh"
#include "common/futex.hh"
#include "common/ipc_message.hh"
#include "common/slab.hh"
#include "common/util.hh"
#include "interceptor/file.hh"
#include "libsyscall_intercept_hook_point.h"
#include <cstdint>
#include <sys/types.h>
namespace nextfs {
__attribute__((constructor)) auto init_preload() -> void;

class PreloadContext {
private:
  OpenFileManager open_file_manager_;

public:
  ~PreloadContext();
  [[nodiscard]] static auto open_file_manager() -> OpenFileManager & {
    return instance().open_file_manager_;
  }

public:
  using ipc_channel_type = GDRing<IpcMessageInfo>;
  // using ipc_channel_type = ConcurrentRing<IpcMessageInfo, ENQ_MP, DEQ_SC>;
  using ipc_pool_type = StaticSlab;
  using block_pool_type = StaticSlab;

public:
  ipc_channel_type *ipc_channel_;
  ipc_pool_type *ipc_obj_pool_;
  FutexParker *ipc_futex_;
  block_pool_type *block_pool_;
  Config config_;
  std::string cwd_;
  int ipc_channel_shm_id_{-1};
  int ipc_obj_pool_shm_id_{-1};
  uint32_t node_id_;

private:
  // PreloadContext();

public:
  static auto init() -> int;
  static auto instance() -> PreloadContext & {
    static PreloadContext instance;
    return instance;
  }
  static auto ipc_channel() -> ipc_channel_type * {
    return instance().ipc_channel_;
  }
  static auto ipc_obj_pool() -> ipc_pool_type * {
    return instance().ipc_obj_pool_;
  }
  static auto ipc_futex() -> FutexParker * { return instance().ipc_futex_; }
  static auto block_pool() -> block_pool_type * {
    return instance().block_pool_;
  }
  static auto node_id() -> uint32_t { return instance().node_id_; }
  static auto config() -> Config { return instance().config_; }
  static auto cwd() -> const std::string & { return instance().cwd_; }
  static auto mount_point() -> const std::string & {
    return instance().config_.mount_point_;
  }
};
} // namespace nextfs