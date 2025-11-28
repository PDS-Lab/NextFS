#pragma once

#include "common/futex.hh"
#include "common/util.hh"
#include "interceptor/file.hh"
#include "metadata/inode.hh"
#include "vector"
#include <cassert>
#include <cstdint>
#include <future>
#include <sys/types.h>
namespace nextfs {

constexpr static auto msg_head_size = 220;
constexpr static auto ino_cache_level = 5;

// test
enum class RpcMessageType : uint16_t {
  RPC_TYPE_SYNC = 400,
  RPC_TYPE_READ,
  RPC_TYPE_STAT,
  RPC_TYPE_UNLINK,
  RPC_TYPE_FIND,
  RPC_TYPE_STATFS,

  RPC_TYPE_SPEED_TEST,

  // Metadata related
  RPC_TYPE_ADD_DENTRY_REQ,
  RPC_TYPE_ADD_DENTRY_RESP,
  RPC_TYPE_REMOVE_DENTRY_REQ,
  RPC_TYPE_REMOVE_DENTRY_RESP,

  RPC_TYPE_CREATE_INODE_REQ,
  RPC_TYPE_CREATE_INODE_RESP,

  RPC_TYPE_METADATA_PARSE_PATH_REQ,
  RPC_TYPE_METADATA_PARSE_PATH_RESP,

  RPC_TYPE_GET_INODE_REQ,
  RPC_TYPE_GET_INODE_RESP,

  RPC_TYPE_WRITE_REQ,
  RPC_TYPE_WRITE_RESP,

  RPC_TYPE_INLINE_WRITE_REQ,
  RPC_TYPE_INLINE_WRITE_RESP,

  RPC_TYPE_INLINE_DATA_MIGRATE_REQ,
  RPC_TYPE_INLINE_DATA_MIGRATE_RESP,

  RPC_TYPE_UPDATE_INODE_REQ,
  RPC_TYPE_UPDATE_INODE_RESP,

  RPC_TYPE_READ_REQ,
  RPC_TYPE_READ_RESP,

  RPC_TYPE_DELETE_DENTRY_REQ,
  RPC_TYPE_DELETE_INODE_REQ,
  RPC_TYPE_DELETE_DATA_REQ,
  RPC_TYPE_DELETE_INODE_RESP,
  RPC_TYPE_DELETE_DENTRY_RESP,

  RPC_TYPE_ADD_OVERFLOW_ATABLE_ENTRY_REQ,
  RPC_TYPE_ADD_OVERFLOW_ATABLE_ENTRY_RESP,

  RPC_TYPE_GET_OVERFLOW_ATABLE_ENTRY_REQ,
  RPC_TYPE_GET_OVERFLOW_ATABLE_ENTRY_RESP,
};

// 24B
struct Notify {
  uint64_t complete_addr_; // use *complete_addr_ = true to notify that the
                           // rpc is completed
  uint64_t futex_addr_;    // use *futex_addr_.unpark() to wake up
  uint64_t resp_addr_;     // use *resp_addr_ to store the rpc resp
};

struct ReadNotify : Notify {
  uint64_t msg_addr_;
  uint64_t data_addr_;
};

struct BaseRpcMsg {
  RpcMessageType type_;
};

struct RequestParsePath : BaseRpcMsg {
  uint32_t client_id_;              // the origin client id
  char remained_path_[max_str_len]; // the remained path waiting for parsing
  ino_type cur_ino_;                // The latest parsed ino

  ino_type medi_ino_[ino_cache_level]{}; // cache the intermediate results
  uint32_t cur_level_{};
  Notify notify_;
};

struct RequestGetInode : BaseRpcMsg {
  uint32_t client_id_; // the origin client id
  ino_type file_ino_;  // file ino

  ReadNotify notify_;
};

struct RequestAddDentry : BaseRpcMsg {
  uint32_t client_id_;
  dentry_key dent_key_;
  dentry dent_;

  Notify notify_;
};

struct RequestRemoveDentry : BaseRpcMsg {
  uint32_t client_id_;
  dentry_key dent_key_;

  Notify notify_;
};

struct RequestCreateInode : BaseRpcMsg {
  uint32_t client_id_;
  ino_type ino_;
  bool is_dir_;

  Notify notify_;
};

// msg for write:
// RequestWrite + data
struct RequestWrite : BaseRpcMsg {
  uint32_t client_id_;
  ino_type file_ino_;
  uint32_t block_num_; // write to which block
  uint32_t offset_;    // offset in block
  uint32_t data_size_;

  Notify notify_;

  // char data_[]; // flexible array
};

// msg for inline write:
// RequestInlineWrite + data
struct RequestInlineWrite : BaseRpcMsg {
  uint32_t client_id_;
  ino_type file_ino_;
  uint16_t offset_;
  uint16_t data_size_;

  Notify notify_;

  // char data_[]; // flexible array
};

struct RequestInlineDataMigrate : BaseRpcMsg {
  uint32_t client_id_;
  ino_type file_ino_;

  ReadNotify notify_;
};

// msg for update inode:
// RequestUpdateInode + data
// put node_ids in data
struct RequestUpdateInode : BaseRpcMsg {
  uint32_t client_id_;
  ino_type file_ino_;
  uint64_t file_size_; // new file size
  // uint32_t block_num_; // update block_num_ -> nodeid in ATable
  uint32_t block_cnt_;
  // uint64_t send_time_;
  Notify notify_;

  // uint32_t node_ids_[];
};

struct RequestRead : BaseRpcMsg {
  uint32_t client_id_;
  ino_type file_ino_;
  uint32_t block_num_;
  uint32_t offset_; // offset in block
  uint32_t read_size_;

  ReadNotify notify_;
};

struct RequestDeleteDentry : BaseRpcMsg {
  uint32_t client_id_;
  ino_type parent_ino_;
  char file_name_[max_str_len];
  Notify notify_;
};

struct RequestDeleteInode : BaseRpcMsg {
  uint32_t client_id_;
  ino_type file_ino_;
  Notify notify_;
};

struct RequestDeleteData : BaseRpcMsg {
  // uint32_t client_id_;
  ino_type file_ino_;
  uint32_t block_num_;
  // no Notify
};

struct RequestAddOverflowATableEntry : BaseRpcMsg {
  uint32_t client_id_;
  ino_type file_ino_;
  uint32_t block_num_;
  uint32_t node_id_;

  Notify notify_;
};

struct RequestGetOverflowATableEntry : BaseRpcMsg {
  uint32_t client_id_;
  ino_type file_ino_;
  uint32_t block_num_;

  Notify notify_;
};

enum class MetadataRespMsg : uint16_t {
  PARSE_PATH_SUCCESS = 500,
  PARSE_PATH_FAILED_BUT_FIND_PARENT,
  PARSE_PATH_FAILED_NOT_FIND_PARENT,
  PARSE_PATH_FAILED_RPC_WRONG_NODE,

  INODE_FOUND,
  INODE_NOT_FOUND,

  ADD_DENTRY_SUCCESS,
  ADD_DENTRY_FAILED,
  DENTRY_ALREADY_EXIST,

  REMOVE_DENTRY_SUCCESS,
  REMOVE_DENTRY_FAILED,
  DENTRY_NOT_EXIST,

  CREATE_INODE_SUCCESS,
  CREATE_INODE_FAILED,

  DELETE_INODE_SUCCESS,
  DELETE_INODE_FAILED,

  DELETE_DENTRY_SUCCESS,
  DELETE_DENTRY_FAILED,

  WRITE_SUCCESS,
  WRITE_BTABLE_FAILED,
  WRITE_DATA_TOO_LONG,
  WRITE_DATA_FAILED,

  INLINE_WRITE_SUCCESS,
  INLINE_WRITE_DATA_TOO_LONG,
  INLINE_WRITE_INODE_NOT_FOUND,
  INLINE_WRITE_DATA_FAILED,

  INLINE_DATA_MIGRATE_SUCCESS,
  INLINE_DATA_MIGRATE_INODE_NOT_FOUND,
  INLINE_DATA_MIGRATE_INODE_IS_DIR,
  INLINE_DATA_MIGRATE_ATABLE_ALREADY_EXISTS,
  INLINE_DATA_MIGRATE_FAILED,

  UPDATE_INODE_SUCCESS,
  INODE_NOT_IN_THIS_NODE,
  UPDATE_INODE_FAILED,

  READ_SUCCESS,
  BTABLE_NOT_FIND_THE_BLOCK,
  READ_FAILED,

  ADD_OVERFLOW_ATABLE_ENTRY_SUCCESS,
  ADD_OVERFLOW_ATABLE_ENTRY_FAILED,
  ADD_OVERFLOW_ATABLE_ENTRY_EXISTS,

  GET_OVERFLOW_ATABLE_ENTRY_SUCCESS,
  GET_OVERFLOW_ATABLE_ENTRY_FAILED,
};

struct ResponseClose : BaseRpcMsg {
  MetadataRespMsg msg_;
  Notify notify_;
};

struct ResponseParsePath : BaseRpcMsg {
  MetadataRespMsg msg_;
  ino_type target_ino_;
  ino_type medi_ino_[ino_cache_level];

  Notify notify_; // equals the notify_ of RequestParsePath
};

struct ResponseCreateInode : BaseRpcMsg {
  MetadataRespMsg msg_;
  Notify notify_;
};

// response of get_inode:
// ResponseGetInode + data (DirInode) or
// ResponseGetInode + data(FileInode)
struct ResponseGetInode : BaseRpcMsg {
  MetadataRespMsg msg_;

  bool is_dir_;
  // DirInode dir_inode_;
  // FileInode file_inode_;
  ReadNotify notify_; // equals the notify_ of RequestInode
};

struct ResponseAddDentry : BaseRpcMsg {
  MetadataRespMsg msg_;
  ino_type exist_ino_;
  Notify notify_;
};

struct ResponseRemoveDentry : BaseRpcMsg {
  MetadataRespMsg msg_;
  dentry dent_; // dentry before remove
  Notify notify_;
};

struct ResponseWrite : BaseRpcMsg {
  MetadataRespMsg msg_;
  uint32_t write_size_;
  // uint32_t target_block_size_;
  Notify notify_;
};

struct ResponseInlineWrite : BaseRpcMsg {
  MetadataRespMsg msg_;
  uint32_t write_size_;
  uint32_t inline_data_size_;
  Notify notify_;
};

struct ResponseInlineDataMigrate : BaseRpcMsg {
  MetadataRespMsg msg_;
  // FileInode file_inode_;
  ReadNotify notify_;
};

struct ResponseUpdateInode : BaseRpcMsg {
  MetadataRespMsg msg_;
  // uint64_t send_time_;
  Notify notify_;
};

// response of read:
// ResponseRead + data
struct ResponseRead : BaseRpcMsg {
  MetadataRespMsg msg_;
  uint32_t read_size_;
  ReadNotify notify_;

  // char data_[]; // flexible array
};

struct ResponseDeleteDentry : BaseRpcMsg {
  MetadataRespMsg msg_;
  ino_type file_ino_; // before delete
  Notify notify_;
};

struct ResponseDeleteInode : BaseRpcMsg {
  MetadataRespMsg msg_;
  Notify notify_;
};

struct ResponseAddOverflowATableEntry : BaseRpcMsg {
  MetadataRespMsg msg_;

  Notify notify_;
};

struct ResponseGetOverflowATableEntry : BaseRpcMsg {
  MetadataRespMsg msg_;
  uint32_t node_id_;

  Notify notify_;
};

/*
  RpcMessage
*/
// union RpcData {
//   // Request
//   RequestParsePath parse_path_req_;
//   RequestGetInode get_inode_req_;
//   RequestAddDentry add_dentry_req_;
//   RequestRemoveDentry rm_dentry_req_;
//   RequestCreateInode create_inode_req_;
//   RequestWrite write_req_;
//   RequestUpdateInode update_inode_req_;
//   RequestRead read_req_;

//   // Response
//   ResponseParsePath parse_path_resp_;
//   ResponseGetInode get_inode_resp_;
//   ResponseAddDentry add_dentry_resp_;
//   ResponseRemoveDentry rm_dentry_resp_;
//   ResponseCreateInode create_inode_resp_;
//   ResponseWrite write_resp_;
//   ResponseUpdateInode update_inode_resp_;
//   ResponseRead read_resp_;

//   RpcData(){};
//   ~RpcData(){};
// };

// struct RpcMessage {
//   RpcMessageType type_;
//   RpcData data_;
// };

} // namespace nextfs