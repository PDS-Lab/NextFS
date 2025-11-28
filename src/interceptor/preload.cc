#include "interceptor/preload.hh"
#include "common/config.hh"
#include "common/futex.hh"
#include "common/ipc_message.hh"
#include "common/logger.hh"
#include "interceptor/hook.hh"
#include <cstdlib>
#include <linux/limits.h>
#include <sys/mman.h>
#include <sys/shm.h>
#include <unistd.h>
namespace nextfs {

auto init_preload() -> void {
  PreloadContext::instance().init();
  intercept_hook_point = hook_wrapper;
}

auto PreloadContext::init() -> int {
  auto &ctx = instance();
  ctx.config_ = parse_config(getenv(nextfs::CONFIG_ENV));
  GlobalLogger::instance().init(ctx.config_.log_level_);
  ctx.node_id_ = ctx.config_.node_id_;

  char buffer[PATH_MAX];
  if (getcwd(buffer, PATH_MAX) == nullptr) {
    fmt::print(stderr, "getcwd failed\n");
    return -1;
  }
  ctx.cwd_ = buffer;

  //! ipc channel
  size_t chnl_space =
      ipc_channel_type::required_space(ctx.config_.ipc_channel_size_);
  int fd = shm_open(ctx.config_.ipc_chnl_filename_.c_str(), O_RDWR,
                    S_IRUSR | S_IWUSR);
  if (fd == -1) {
    trace("{}", ctx.config_.ipc_chnl_filename_);
    warn("failed to create ipc channel file, {}", std::strerror(errno));
    return -1;
  }
  // do we need huge table?
  void *addr =
      mmap(nullptr, chnl_space, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (unlikely(addr == (void *)-1)) {
    shm_unlink(ctx.config_.ipc_chnl_filename_.c_str());
    warn("failed to mmap addr for ipc channel. {}", std::strerror(errno));
    return -1;
  }
  ctx.ipc_channel_ =
      ipc_channel_type::create_ring(ctx.config_.ipc_channel_size_, addr, false);

  //! ipc object pool
  size_t ipc_pool_space = ipc_pool_type::required_space(
      ctx.config_.ipc_pool_size_, sizeof(IpcMessage));
  fd = shm_open(ctx.config_.ipc_pool_filename_.c_str(), O_RDWR,
                S_IRUSR | S_IWUSR);
  if (fd == -1) {
    warn("failed to create ipc pool file, {}", std::strerror(errno));
    return -1;
  }
  // do we need huge table?
  addr =
      mmap(nullptr, ipc_pool_space, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (unlikely(addr == (void *)-1)) {
    shm_unlink(ctx.config_.ipc_pool_filename_.c_str());
    warn("failed to mmap addr for ipc pool. {}", std::strerror(errno));
    return -1;
  }
  ctx.ipc_obj_pool_ = ipc_pool_type::create_slab(
      ctx.config_.ipc_pool_size_, sizeof(IpcMessage), addr, false);

  // !futex
  fd = shm_open(ctx.config_.ipc_futex_filename_.c_str(), O_RDWR,
                S_IRUSR | S_IWUSR);
  if (fd == -1) {
    warn("failed to create ipc futex file, {}", std::strerror(errno));
    return -1;
  }
  addr = mmap(nullptr, sizeof(FutexParker), PROT_READ | PROT_WRITE, MAP_SHARED,
              fd, 0);
  if (unlikely(addr == (void *)-1)) {
    shm_unlink(ctx.config_.ipc_futex_filename_.c_str());
    warn("failed to mmap addr for ipc pool. {}", std::strerror(errno));
    return -1;
  }
  ctx.ipc_futex_ = reinterpret_cast<FutexParker *>(addr);

  //! block pool
  size_t block_pool_space = block_pool_type::required_space(
      ctx.config_.block_pool_size_, ctx.config_.block_size_);
  fd = shm_open(ctx.config_.block_pool_filename_.c_str(), O_RDWR,
                S_IRUSR | S_IWUSR);
  if (fd == -1) {
    warn("failed to create block pool file, {}", std::strerror(errno));
    return -1;
  }
  addr = mmap(nullptr, block_pool_space, PROT_READ | PROT_WRITE, MAP_SHARED, fd,
              0);
  if (unlikely(addr == (void *)-1)) {
    shm_unlink(ctx.config_.block_pool_filename_.c_str());
    warn("failed to mmap addr for block pool. {}", std::strerror(errno));
    return -1;
  }
  ctx.block_pool_ = block_pool_type::create_slab(
      ctx.config_.block_pool_size_, ctx.config_.block_size_, addr, false);
  return 0;
}

PreloadContext::~PreloadContext() = default;
} // namespace nextfs