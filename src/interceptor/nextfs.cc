#include "interceptor/nextfs.hh"
#include "common/ipc_message.hh"
#include "common/logger.hh"
#include "common/util.hh"
#include "interceptor/file.hh"
#include "interceptor/preload.hh"
#include "interceptor/util.hh"
#include "metadata/inode.hh"
#include <asm-generic/errno-base.h>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <future>
#include <sys/types.h>
#include <utility>
namespace nextfs {

struct DeferFreeObj {
  int pool_index;
  DeferFreeObj(int pool_index) : pool_index(pool_index){};
  ~DeferFreeObj() { PreloadContext::ipc_obj_pool()->free(pool_index); }
};

static void fill_stat(struct stat *buf, const statistic_message &msg) {
  buf->st_dev = 0;
  buf->st_ino = msg.ino.hash();
  buf->st_mode = msg.perm_; // TODO: perm is always 7 now
  buf->st_nlink = 1;
  buf->st_uid = 0;
  buf->st_gid = 0;
  buf->st_rdev = 0;
  buf->st_size = msg.file_size_;
  buf->st_blksize = PreloadContext::config().block_size_;
  buf->st_blocks = (buf->st_size + buf->st_blksize - 1) / buf->st_blksize;
  buf->st_atim.tv_sec = msg.access_time_ / 1000000000;
  buf->st_atim.tv_nsec = msg.access_time_ % 1000000000;
  buf->st_mtim.tv_sec = msg.modify_time_ / 1000000000;
  buf->st_mtim.tv_nsec = msg.modify_time_ % 1000000000;
  buf->st_ctim.tv_sec = msg.create_time_ / 1000000000;
  buf->st_ctim.tv_nsec = msg.create_time_ % 1000000000;
}

// mkdir success return 0; fail return -1;
auto nextfs_mkdirat(int dirfd [[maybe_unused]], std::string_view pathname,
                    mode_t mode) -> int {
  if (pathname.size() >= MAX_PATH_LEN) {
    warn("pathname {} too long", pathname);
    errno = ENAMETOOLONG;
    return -1;
  }

  auto ipc_channel = PreloadContext::ipc_channel();
  auto ipc_pool = PreloadContext::ipc_obj_pool();
  auto [pool_index, ipc_msg] = ipc_pool->alloc<IpcMessage>().value();
  DeferFreeObj g(pool_index);
  new (ipc_msg) IpcMessage(); // init msg
  auto msg = &ipc_msg->data_.mkdirMSg;

  memcpy(msg->req_.path_string_, pathname.data(), pathname.size());
  msg->req_.path_string_[pathname.size()] = '\0';
  msg->req_.mode_ = mode;

  IpcMessageInfo info{IpcMessageType::IPC_MSG_MKDIR, pool_index};
  submit_request(info, ipc_channel);
  wait_response(ipc_msg->sync_);
  auto pre = ipc_msg->sync_.state_.exchange(IpcState::IPC_STATE_CLIENT_DONE,
                                            std::memory_order_acq_rel);
  if (pre != IpcState::IPC_STATE_DAEMON_DONE) {
    warn("IpcState should be DAEMON DONE while is {}", (int)pre);
  }
  auto &resp = msg->resp_;
  if (resp.RespMsg == FileRespMsg::MKDIR_SUCCESS) {
    trace("nextfs mkdir success");
    return 0;
  } else if (resp.RespMsg == FileRespMsg::MKDIR_FAIL) {
    warn("nextfs mkdir fail");
    return -1;
  }
  __builtin_unreachable();
}

// open success return fd; fail return -1;
auto nextfs_openat(std::string_view pathname, int flags, mode_t mode) -> int {

  if (flags & (O_PATH | O_DIRECTORY | O_APPEND)) {
    error("O_PATH, O_DIRECTORY, O_APPEND not supported");
    errno = ENOTSUP;
    return -1;
  }
  if (pathname.size() > MAX_PATH_LEN) {
    error("pathname {} too long", pathname);
    errno = ENAMETOOLONG;
    return -1;
  }
  if (flags & O_TRUNC) {
    // TODO: call mmfs_truncate
  }

  auto ipc_channel = PreloadContext::ipc_channel();
  auto ipc_pool = PreloadContext::ipc_obj_pool();
  auto [pool_index, ipc_msg] = ipc_pool->alloc<IpcMessage>().value();
  DeferFreeObj g(pool_index);
  new (ipc_msg) IpcMessage(); // init msg
  auto msg = &ipc_msg->data_.openatMsg;
  memcpy(msg->req_.path_string_, pathname.data(), pathname.size());
  msg->req_.path_string_[pathname.size()] = '\0';
  trace("pool_index:{} ipc_pool req->path :{}", (int)pool_index,
        msg->req_.path_string_);
  msg->req_.mode_ = mode;
  msg->req_.flags_ = flags;
  IpcMessageInfo info{IpcMessageType::IPC_MSG_OPEN, pool_index};
  submit_request(info, ipc_channel);
  wait_response(ipc_msg->sync_);
  trace("nextfs_openat() get resp");
  auto pre = ipc_msg->sync_.state_.exchange(IpcState::IPC_STATE_CLIENT_DONE,
                                            std::memory_order_acq_rel);
  if (pre != IpcState::IPC_STATE_DAEMON_DONE) {
    warn("IpcState should be DAEMON DONE while is {}", (int)pre);
  }
  switch (msg->resp_.RespMsg) {
  case FileRespMsg::OPEN_SUCCESS_WITHOUT_CREATE:
  case FileRespMsg::OPEN_SUCCESS_WITH_CREATE: {
    int fd = PreloadContext::open_file_manager().open(
        msg->resp_.ino, flags, mode, msg->req_.path_string_, 0);
    trace("nextfs open success, fd = {}", fd);
    return fd;
  }
  case FileRespMsg::OPEN_FAIL_FILE_EXIST:
    errno = EEXIST;
    return -1;
  case FileRespMsg::OPEN_FAIL_FILE_NOT_EXIST:
    errno = ENOENT;
    return -1;
  default:
    errno = EIO;
    return -1;
  }
}

// close success return 0; fail return -1;
auto nextfs_close(const std::shared_ptr<FileInfo> &file) -> int {
  auto ipc_channel = PreloadContext::ipc_channel();
  auto ipc_pool = PreloadContext::ipc_obj_pool();
  auto [pool_index, ipc_msg] = ipc_pool->alloc<IpcMessage>().value();
  DeferFreeObj g(pool_index);
  new (ipc_msg) IpcMessage(); // init msg
  auto msg = &ipc_msg->data_.closeMsg;

  msg->req_.file_ino = file->ino();

  IpcMessageInfo info{IpcMessageType::IPC_MSG_CLOSE, pool_index};
  submit_request(info, ipc_channel);
  wait_response(ipc_msg->sync_);
  auto pre = ipc_msg->sync_.state_.exchange(IpcState::IPC_STATE_CLIENT_DONE,
                                            std::memory_order_acq_rel);
  if (pre != IpcState::IPC_STATE_DAEMON_DONE) {
    warn("IpcState should be DAEMON DONE while is {}", (int)pre);
  }
  PreloadContext::open_file_manager().close(file->fd()); // close it anyway
  if (msg->resp_.RespMsg == FileRespMsg::FILE_NOT_OPENED) {
    warn("nextfs file not opened");
    errno = EBADF;
    return -1;
  }
  return 0;
}

auto nextfs_rename(std::string_view old_pathname, std::string_view new_pathname)
    -> int {
  if (old_pathname.size() >= MAX_PATH_LEN) {
    warn("pathname {} too long", old_pathname);
    errno = ENAMETOOLONG;
    return -1;
  }
  if (new_pathname.size() >= MAX_PATH_LEN) {
    warn("pathname {} too long", new_pathname);
    errno = ENAMETOOLONG;
    return -1;
  }
  auto ipc_channel = PreloadContext::ipc_channel();
  auto ipc_pool = PreloadContext::ipc_obj_pool();
  auto [pool_index, ipc_msg] = ipc_pool->alloc<IpcMessage>().value();
  DeferFreeObj g(pool_index);
  new (ipc_msg) IpcMessage(); // init msg
  auto msg = &ipc_msg->data_.renameMsg;
  memcpy(msg->req_.old_pathname_, old_pathname.data(), old_pathname.size());
  msg->req_.old_pathname_[old_pathname.size()] = '\0';
  memcpy(msg->req_.new_pathname_, new_pathname.data(), new_pathname.size());
  msg->req_.new_pathname_[new_pathname.size()] = '\0';
  trace("pool_index:{} ipc_pool req->old_path :{}   req->new_path :{}",
        (int)pool_index, msg->req_.old_pathname_, msg->req_.new_pathname_);
  IpcMessageInfo info{IpcMessageType::IPC_MSG_RENAME, pool_index};
  submit_request(info, ipc_channel);
  wait_response(ipc_msg->sync_);
  auto pre = ipc_msg->sync_.state_.exchange(IpcState::IPC_STATE_CLIENT_DONE,
                                            std::memory_order_acq_rel);
  if (pre != IpcState::IPC_STATE_DAEMON_DONE) {
    warn("IpcState should be DAEMON DONE while is {}", (int)pre);
  }
  if (msg->resp_.RespMsg == FileRespMsg::RENAME_SUCCESS) {
    return 0;
  }
  errno = EIO;
  return -1;
}

// stat success return 0; fail return -1;
auto nextfs_stat(std::string_view pathname, struct stat *buf) -> int {
  if (pathname.size() >= MAX_PATH_LEN) {
    warn("pathname {} too long", pathname);
    errno = ENAMETOOLONG;
    return -1;
  }
  auto ipc_channel = PreloadContext::ipc_channel();
  auto ipc_pool = PreloadContext::ipc_obj_pool();
  auto [pool_index, ipc_msg] = ipc_pool->alloc<IpcMessage>().value();
  DeferFreeObj g(pool_index);
  new (ipc_msg) IpcMessage(); // init msg
  auto msg = &ipc_msg->data_.statMsg;
  memcpy(msg->req_.path_string_, pathname.data(), pathname.size());
  msg->req_.path_string_[pathname.size()] = '\0';
  msg->req_.use_ino_ = false;
  IpcMessageInfo info{IpcMessageType::IPC_MSG_STAT, pool_index};
  submit_request(info, ipc_channel);
  wait_response(ipc_msg->sync_);
  auto pre = ipc_msg->sync_.state_.exchange(IpcState::IPC_STATE_CLIENT_DONE,
                                            std::memory_order_acq_rel);
  if (pre != IpcState::IPC_STATE_DAEMON_DONE) {
    warn("IpcState should be DAEMON DONE while is {}", (int)pre);
  }
  if (msg->resp_.RespMsg == FileRespMsg::STAT_FAIL) {
    warn("nextfs stat fail: {}", pathname);
    return -1;
  }
  if (buf != nullptr) {
    fill_stat(buf, msg->resp_.statMessage);
  }
  return 0;
}

auto nextfs_fstat(const std::shared_ptr<FileInfo> &file, struct stat *buf)
    -> int {
  auto ipc_channel = PreloadContext::ipc_channel();
  auto ipc_pool = PreloadContext::ipc_obj_pool();
  auto [pool_index, ipc_msg] = ipc_pool->alloc<IpcMessage>().value();
  DeferFreeObj g(pool_index);
  new (ipc_msg) IpcMessage(); // init msg
  auto msg = &ipc_msg->data_.statMsg;
  msg->req_.ino_ = file->ino();
  msg->req_.use_ino_ = true;
  IpcMessageInfo info{IpcMessageType::IPC_MSG_STAT, pool_index};
  submit_request(info, ipc_channel);
  wait_response(ipc_msg->sync_);
  auto pre = ipc_msg->sync_.state_.exchange(IpcState::IPC_STATE_CLIENT_DONE,
                                            std::memory_order_acq_rel);
  if (pre != IpcState::IPC_STATE_DAEMON_DONE) {
    warn("IpcState should be DAEMON DONE while is {}", (int)pre);
  }
  if (msg->resp_.RespMsg == FileRespMsg::STAT_FAIL) {
    warn("nextfs stat fail");
    return -1;
  }
  if (buf != nullptr) {
    fill_stat(buf, msg->resp_.statMessage);
  }
  return 0;
}

// fsync success return 0; fail return -1;
// auto nextfs_fsync(const std::shared_ptr<FileInfo> &file) -> int {
//   auto group_id = file->ino().group_id();
//   auto server_id = file->ino().server_id();
//   // should block until responded
//   auto ipc_channel = PreloadContext::ipc_channel();
//   auto ipc_pool = PreloadContext::ipc_obj_pool();
//   auto pool_index = ipc_pool->alloc();
//   IpcFsyncMsg *msg = &(ipc_pool->get(pool_index).fsyncMsg);
//   ipc_msg->sync_.state_.store(IpcState::IPC_STATE_PENDING,
//   std::memory_order_release); msg->req_.ino_ = file->ino(); IpcMessageInfo
//   info{IpcMessageType::IPC_MSG_FSYNC, pool_index}; submit_request(info,
//   ipc_channel); wait_response(ipc_msg->sync_); auto pre =
//   ipc_msg->sync_.state_.exchange(IpcState::IPC_STATE_CLIENT_DONE,
//                                   std::memory_order_acq_rel);
//   if (pre != IpcState::IPC_STATE_DAEMON_DONE) {
//     warn("IpcState should be DAEMON DONE while is {}", (int)pre);
//   }
//   if (msg->resp_.RespMsg == FileRespMsg::FSYNC_SUCCESS) {
//     return 0;
//   } else {
//     warn("nextfs fsync fail");
//     return -1;
//   }
// }

auto nextfs_pread(const std::shared_ptr<FileInfo> &file, void *buf,
                  size_t count, off_t offset) -> ssize_t {
  if (file->flags() & O_WRONLY) {
    error("read from write only file");
    errno = EBADF;
    return -1;
  }
  if (file->mode() & S_IFDIR) {
    error("read from directory");
    errno = EISDIR;
    return -1;
  }
  if (count == 0) {
    return 0;
  }
  if (offset < 0) {
    error("offset {} is negative", offset);
    errno = EINVAL;
    return -1;
  }

  auto ipc_channel = PreloadContext::ipc_channel();
  auto ipc_pool = PreloadContext::ipc_obj_pool();
  auto [pool_index, ipc_msg] = ipc_pool->alloc<IpcMessage>().value();
  //!!! daemon will handle ipc_msg free, so we don't use DeferFreeObj here
  new (ipc_msg) IpcMessage(); // init msg
  auto msg = &ipc_msg->data_.readMsg;
  msg->req_.ino_ = file->ino();
  msg->req_.count_ = count;
  msg->req_.offset_ = offset;
  submit_request(IpcMessageInfo{IpcMessageType::IPC_MSG_READ, pool_index},
                 ipc_channel);
  ssize_t n = 0;
  while (true) {
    IpcState old_state = wait_response(ipc_msg->sync_);
    if (msg->resp_.RespMsg != FileRespMsg::READ_SUCCESS) {
      error("read {} failed at: {} bytes", file->fd(), n);
      ipc_msg->sync_.state_.store(IpcState::IPC_STATE_CLIENT_DONE,
                                  std::memory_order_release);
      errno = EIO;
      n = -1;
      break;
    }
    for (size_t i = 0; i < msg->resp_.block_num_; i++) {
      auto addr = PreloadContext::block_pool()->get<void>(
          msg->resp_.blocks_[i].idx_in_pool_);
      memcpy(static_cast<uint8_t *>(buf) + n, addr, msg->resp_.blocks_[i].len_);
      n += msg->resp_.blocks_[i].len_;
    }
    ipc_msg->sync_.state_.store(old_state == IpcState::IPC_STATE_DAEMON_DONE
                                    ? IpcState::IPC_STATE_CLIENT_DONE
                                    : IpcState::IPC_STATE_PENDING);
    if (old_state == IpcState::IPC_STATE_DAEMON_DONE) {
      break;
    }
  }
  return n;
}

auto nextfs_read(const std::shared_ptr<FileInfo> &file, void *buf, size_t count)
    -> ssize_t {
  off_t offset = file->offset();
  ssize_t res = nextfs_pread(file, buf, count, offset);
  if (res > 0) {
    file->set_offset(offset + res);
  }
  return res;
}

auto nextfs_pwrite(const std::shared_ptr<FileInfo> &file, const void *buf,
                   size_t count, off_t offset) -> ssize_t {

  if (file->flags() & O_RDONLY) {
    error("write to read only file");
    errno = EBADF;
    return -1;
  }
  if (file->mode() & S_IFDIR) {
    error("write to directory");
    errno = EISDIR;
    return -1;
  }
  if (count == 0) {
    return 0;
  }
  if (offset < 0) {
    error("offset {} is negative", offset);
    errno = EINVAL;
    return -1;
  }

  auto ipc_channel = PreloadContext::ipc_channel();
  auto ipc_pool = PreloadContext::ipc_obj_pool();
  auto [pool_index, ipc_msg] = ipc_pool->alloc<IpcMessage>().value();
  //!!! daemon will handle ipc_msg free, so we don't use DeferFreeObj here
  new (ipc_msg) IpcMessage(); // init msg
  auto msg = &ipc_msg->data_.writeMsg;

  msg->req_.ino_ = file->ino();
  msg->req_.count_ = count;
  msg->req_.offset_ = offset;

  submit_request(IpcMessageInfo{IpcMessageType::IPC_MSG_WRITE, pool_index},
                 ipc_channel);

  ssize_t n = 0;

  while (true) {
    IpcState old_state = wait_response(ipc_msg->sync_);
    if (msg->resp_.RespMsg != FileRespMsg::WRITE_SUCCESS) {
      error("write {} failed at {} bytes", file->fd(), n);
      ipc_msg->sync_.state_.store(IpcState::IPC_STATE_CLIENT_DONE,
                                  std::memory_order_release);
      errno = EIO;
      n = -1;
      break;
    }
    for (size_t i = 0; i < msg->resp_.block_num_; i++) {
      auto addr = PreloadContext::block_pool()->get<void>(
          msg->resp_.blocks_[i].idx_in_pool_);

      memcpy(addr, static_cast<const uint8_t *>(buf) + n,
             msg->resp_.blocks_[i].len_);

      n += msg->resp_.blocks_[i].len_;
    }
    ipc_msg->sync_.state_.store(old_state == IpcState::IPC_STATE_DAEMON_DONE
                                    ? IpcState::IPC_STATE_CLIENT_DONE
                                    : IpcState::IPC_STATE_PENDING);
    if (old_state == IpcState::IPC_STATE_DAEMON_DONE) {
      break;
    }
  }
  return n;
}

auto nextfs_write(const std::shared_ptr<FileInfo> &file, const void *buf,
                  size_t count) -> ssize_t {
  off_t offset = file->offset();
  ssize_t res = nextfs_pwrite(file, buf, count, offset);
  if (res > 0) {
    file->set_offset(offset + res);
  }
  return res;
}

auto nextfs_unlink(std::string_view pathname) -> int {
  if (pathname.size() >= MAX_PATH_LEN) {
    warn("pathname {} too long", pathname);
    errno = ENAMETOOLONG;
    return -1;
  }
  auto ipc_channel = PreloadContext::ipc_channel();
  auto ipc_pool = PreloadContext::ipc_obj_pool();
  auto [pool_index, ipc_msg] = ipc_pool->alloc<IpcMessage>().value();
  DeferFreeObj g(pool_index);
  new (ipc_msg) IpcMessage(); // init msg
  auto msg = &ipc_msg->data_.unlinkMsg;
  memcpy(msg->req_.path_string_, pathname.data(), pathname.size());
  msg->req_.path_string_[pathname.size()] = '\0';

  IpcMessageInfo info{IpcMessageType::IPC_MSG_UNLINK, pool_index};
  submit_request(info, ipc_channel);
  wait_response(ipc_msg->sync_);
  auto pre = ipc_msg->sync_.state_.exchange(IpcState::IPC_STATE_CLIENT_DONE,
                                            std::memory_order_acq_rel);
  if (pre != IpcState::IPC_STATE_DAEMON_DONE) {
    warn("IpcState should be DAEMON DONE while is {}", (int)pre);
  }
  if (msg->resp_.RespMsg == FileRespMsg::UNLINK_SUCCESS) {
    return 0;
  } else if (msg->resp_.RespMsg == FileRespMsg::UNLINK_FAIL) {
    warn("nextfs unlink fail");
    return -1;
  } else {
    warn("nextfs result unknown");
    return -1;
  }
}

auto nextfs_lseek(const std::shared_ptr<FileInfo> &file, off_t offset,
                  int whence) -> off_t {

  off_t new_offset = 0;
  switch (whence) {
  case SEEK_SET:
    if (offset < 0) {
      errno = EINVAL;
      return -1;
    }
    new_offset = offset;
    break;
  case SEEK_CUR:
    new_offset = file->offset() + offset;
    if (new_offset < 0) {
      errno = EOVERFLOW;
      return -1;
    }
    break;
  case SEEK_END: {
    // TODO: call mmfs_statx to get file size
    struct stat st;
    if (nextfs_fstat(file, &st) < 0) {
      return -1;
    }
    new_offset = st.st_size + offset;
    if (new_offset < 0) {
      errno = EINVAL;
      return -1;
    }
    break;
  }
  case SEEK_DATA:
  case SEEK_HOLE:
    warn("SEEK_DATA and SEEK_HOLE are not supported");
    errno = EINVAL;
    return -1;
  default:
    error("invalid whence {}", whence);
    errno = EINVAL;
    return -1;
  }
  file->set_offset(new_offset);
  return new_offset;
}
} // namespace nextfs