#pragma once
#include "common/config.hh"
#include "common/futex.hh"
#include "common/util.hh"
#include "interceptor/file.hh"
#include "metadata/inode.hh"
#include "variant"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <future>
#include <spdlog/fmt/bundled/core.h>
namespace nextfs {
enum class FileRespMsg : uint16_t {
  FILE_NOT_OPENED = 500,

  MKDIR_SUCCESS,
  MKDIR_FAIL,

  OPEN_SUCCESS_WITHOUT_CREATE,
  OPEN_SUCCESS_WITH_CREATE,
  OPEN_FAIL_FILE_EXIST,
  OPEN_FAIL_FILE_NOT_EXIST,
  OPEN_FAIL,

  CLOSE_DAEMON_SUCCESS,
  CLOSE_HOOK_FILE,

  RENAME_SUCCESS,
  RENAME_FAIL,

  STAT_SUCCESS,
  STAT_FAIL,

  READ_SUCCESS,
  READ_FAIL,

  WRITE_SUCCESS,
  WRITE_FAIL,

  FSYNC_SUCCESS,

  UNLINK_SUCCESS,
  UNLINK_FAIL

};
enum class IpcMessageType : uint16_t {
  IPC_MSG_MKDIR = 600,
  IPC_MSG_NOP, // for test and benchmark
  IPC_MSG_OPEN,
  IPC_MSG_CLOSE,
  IPC_MSG_RENAME,

  IPC_MSG_READ,
  IPC_MSG_WRITE,

  IPC_MSG_FSYNC,

  IPC_MSG_STAT,
  IPC_MSG_UNLINK,
};

enum class IpcState : uint8_t {
  IPC_STATE_PENDING = 100,
  IPC_STATE_RESP_SEND,
  IPC_STATE_DAEMON_DONE,
  IPC_STATE_CLIENT_DONE,
};

struct Block {
  uint32_t idx_in_pool_;
  // bytes to read/write, always start from offset 0
  uint32_t len_;
};

struct IpcMessageInfo {
  IpcMessageType type_;
  // IpcMessage msg;
  //  std::atomic<IpcState> state_;
  uint32_t pool_index_;
};

struct IpcMessageSync {
  FutexParker parker_;
  std::atomic<IpcState> state_;
  IpcMessageSync() : parker_(false), state_(IpcState::IPC_STATE_PENDING) {}
  ~IpcMessageSync() = default;
};

// mkdir message;
struct IpcMkdiratResp {
  FileRespMsg RespMsg;
  ino_type ino_;
};
struct IpcMkdiratReq {
  // uint8_t dir_num_;
  char path_string_[MAX_PATH_LEN];
  int flags_;
  mode_t mode_;
};
struct IpcMkdiratMsg {
  IpcMkdiratReq req_;
  IpcMkdiratResp resp_;
};

// openat message;
struct IpcOpenatResp {
  ino_type ino;
  FileRespMsg RespMsg;
};
struct IpcOpenatReq {
  char path_string_[MAX_PATH_LEN];
  int flags_;
  mode_t mode_;
};
struct IpcOpenatMsg {
  IpcOpenatReq req_;
  IpcOpenatResp resp_;
};

// close message;
struct IpcCloseResp {
  FileRespMsg RespMsg;
};
struct IpcCloseReq {
  // std::shared_ptr<FileInfo> fi;
  ino_type file_ino;
};
struct IpcCloseMsg {
  IpcCloseReq req_;
  IpcCloseResp resp_;
};
// rename message;
struct IpcRenameResp {
  FileRespMsg RespMsg;
};
struct IpcRenameReq {
  char old_pathname_[MAX_PATH_LEN];
  char new_pathname_[MAX_PATH_LEN];
};
struct IpcRenameMsg {
  IpcRenameReq req_;
  IpcRenameResp resp_;
};

struct statistic_message {
  ino_type ino;
  // size_t filesize;

  Permission perm_{};
  uint64_t create_time_{};
  uint64_t access_time_{};
  uint64_t modify_time_{};
  uint64_t file_size_{};
  // ATable a_table_{}; // ATable is transparent to user
};
// stat message
struct IpcStatResp {
  FileRespMsg RespMsg;
  statistic_message statMessage;
};
struct IpcStatReq {
  char path_string_[MAX_PATH_LEN];
  ino_type ino_;
  bool use_ino_;
};
struct IpcStatMsg {
  IpcStatReq req_;
  IpcStatResp resp_;
};

// // fsync message;
// struct IpcFsyncResp {
//   FileRespMsg RespMsg;
// };
// struct IpcFsyncReq {
//   ino_type ino_;
// };
// struct IpcFsyncMsg {
//
//   IpcFsyncReq req_;
//   IpcFsyncResp resp_;
// };

// read message;
struct IpcReadResp {
  FileRespMsg RespMsg;
  uint8_t block_num_;
  Block blocks_[25];
};
struct IpcReadReq {
  ino_type ino_;
  off_t offset_;
  size_t count_;
};
struct IpcReadMsg {
  IpcReadReq req_;
  IpcReadResp resp_;
};

// write message;
struct IpcWriteResp {
  FileRespMsg RespMsg;
  uint8_t block_num_;
  Block blocks_[25];
};
struct IpcWriteReq {
  ino_type ino_;
  off_t offset_;
  size_t count_;
};
struct IpcWriteMsg {
  IpcWriteReq req_;
  IpcWriteResp resp_;
};

// unlink message;
struct IpcUnlinkResp {
  FileRespMsg RespMsg;
};
struct IpcUnlinkReq {
  char path_string_[MAX_PATH_LEN];
};
struct IpcUnlinkMsg {
  IpcUnlinkReq req_;
  IpcUnlinkResp resp_;
};

// Lseek message;
struct IpcLseekResp {
  FileRespMsg RespMsg;
};
struct IpcLseekReq {
  ino_type ino_;
  ssize_t offset_;
  int whence_;
};
struct IpcLseekMsg {
  IpcLseekReq req_;
  IpcLseekResp resp_;
};

union IpcData {
  // Request
  IpcOpenatMsg openatMsg;
  IpcMkdiratMsg mkdirMSg;
  IpcCloseMsg closeMsg;
  IpcRenameMsg renameMsg;
  IpcStatMsg statMsg;

  // IpcFsyncMsg fsyncMsg;
  IpcReadMsg readMsg;
  IpcWriteMsg writeMsg;
  IpcUnlinkMsg unlinkMsg;
  IpcLseekMsg lseekMsg;
};

struct alignas(CACHE_LINE_SIZE) IpcMessage {
  IpcMessageSync sync_;
  IpcData data_;
  IpcMessage() : data_{} { memset(&data_, 0, sizeof(IpcData)); }
};

static_assert(sizeof(IpcMessage) <= 256, "please adjust IPC message size");

} // namespace nextfs