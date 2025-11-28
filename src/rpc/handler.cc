#include "rpc/handler.hh"
#include "common/config.hh"
#include "common/consistent_hash.hh"
#include "common/ipc_message.hh"
#include "common/memory_pool.hh"
#include "common/util.hh"
#include "daemon/env.hh"
#include "metadata/inode.hh"
#include "rpc/connection.hh"
#include "rpc/message.hh"
#include <ankerl/unordered_dense.h>
#include <cstdint>
#include <cstring>
#include <malloc.h>
#include <string>
namespace nextfs::rpc {

auto handle_recvd_result(RpcHandleArg arg) -> void {
  auto head_blk = arg.head_blk;
  auto data_blk = arg.data_blk;
  void *head_buf = reinterpret_cast<void *>(head_blk->addr_);
  void *data_buf = data_blk.second;

  BaseRpcMsg *bmsg = reinterpret_cast<BaseRpcMsg *>(head_buf);
  auto type = bmsg->type_;
  info("recv msg type : {}", (uint16_t)type);

  switch (type) {
  case RpcMessageType::RPC_TYPE_READ: {
    handle_read_req(head_buf);
  } break;
  case RpcMessageType::RPC_TYPE_FIND: {
    handle_find_req(head_buf);
  } break;
  // case RpcMessageType::RPC_TYPE_STAT: {
  //   handle_stat_req(buf);
  //   break;
  // }
  case RpcMessageType::RPC_TYPE_STATFS: {
    // handle_statfs_req(buf);
  } break;
  case RpcMessageType::RPC_TYPE_SPEED_TEST: {
    // handle_speed_req(buf);
  } break;
  case RpcMessageType::RPC_TYPE_SYNC: {
    // handle_sync_req(buf);
  } break;
  case RpcMessageType::RPC_TYPE_METADATA_PARSE_PATH_REQ: {
    handle_parse_path_req(head_buf);
  } break;
  case RpcMessageType::RPC_TYPE_METADATA_PARSE_PATH_RESP: {
    handle_parse_path_resp(head_buf);
  } break;
  case RpcMessageType::RPC_TYPE_GET_INODE_REQ: {
    handle_get_inode_req(head_buf);
  } break;
  case RpcMessageType::RPC_TYPE_GET_INODE_RESP: {
    handle_get_inode_resp(head_buf, data_buf);
  } break;
  case RpcMessageType::RPC_TYPE_ADD_DENTRY_REQ: {
    handle_add_dentry_req(head_buf);
  } break;
  case RpcMessageType::RPC_TYPE_ADD_DENTRY_RESP: {
    handle_add_dentry_resp(head_buf);
  } break;
  case RpcMessageType::RPC_TYPE_CREATE_INODE_REQ: {
    handle_create_inode_req(head_buf);
  } break;
  case RpcMessageType::RPC_TYPE_CREATE_INODE_RESP: {
    handle_create_inode_resp(head_buf);
  } break;
  case RpcMessageType::RPC_TYPE_REMOVE_DENTRY_REQ: {
    handle_remove_dentry_req(head_buf);
  } break;
  case RpcMessageType::RPC_TYPE_REMOVE_DENTRY_RESP: {
    handle_remove_dentry_resp(head_buf);
  } break;
  case RpcMessageType::RPC_TYPE_WRITE_REQ: {
    handle_write_req(head_buf, data_buf);
  } break;
  case RpcMessageType::RPC_TYPE_WRITE_RESP: {
    handle_write_resp(head_buf);
  } break;
  case RpcMessageType::RPC_TYPE_INLINE_WRITE_REQ: {
    handle_inline_write_req(head_buf, data_buf);
  } break;
  case RpcMessageType::RPC_TYPE_INLINE_WRITE_RESP: {
    handle_inline_write_resp(head_buf);
  } break;
  case RpcMessageType::RPC_TYPE_INLINE_DATA_MIGRATE_REQ: {
    handle_inline_data_migrate_req(head_buf);
  } break;
  case RpcMessageType::RPC_TYPE_INLINE_DATA_MIGRATE_RESP: {
    handle_inline_data_migrate_resp(head_buf, data_buf);
  } break;
  case RpcMessageType::RPC_TYPE_UPDATE_INODE_REQ: {
    handle_update_inode_req(head_buf, data_buf);
  } break;
  case RpcMessageType::RPC_TYPE_UPDATE_INODE_RESP: {
    handle_update_inode_resp(head_buf);
  } break;
  case RpcMessageType::RPC_TYPE_READ_REQ: {
    handle_read_req(head_buf);
  } break;
  case RpcMessageType::RPC_TYPE_READ_RESP: {
    handle_read_resp(head_buf, data_blk);
  } break;
  case RpcMessageType::RPC_TYPE_DELETE_DENTRY_REQ: {
    handle_delete_dentry_req(head_buf);
  } break;
  case RpcMessageType::RPC_TYPE_DELETE_INODE_REQ: {
    handle_delete_inode_req(head_buf);
  } break;
  case RpcMessageType::RPC_TYPE_DELETE_DATA_REQ: {
    handle_delete_data_req(head_buf);
  } break;
  case RpcMessageType::RPC_TYPE_DELETE_INODE_RESP: {
    handle_delete_inode_resp(head_buf);
  } break;
  case RpcMessageType::RPC_TYPE_DELETE_DENTRY_RESP: {
    handle_delete_dentry_resp(head_buf);
  } break;
  case RpcMessageType::RPC_TYPE_ADD_OVERFLOW_ATABLE_ENTRY_REQ: {
    handle_add_overflow_atable_entry_req(head_buf);
  } break;
  case RpcMessageType::RPC_TYPE_ADD_OVERFLOW_ATABLE_ENTRY_RESP: {
    handle_add_overflow_atable_entry_resp(head_buf);
  } break;
  case RpcMessageType::RPC_TYPE_GET_OVERFLOW_ATABLE_ENTRY_REQ: {
    handle_get_overflow_atable_entry_req(head_buf);
  } break;
  case RpcMessageType::RPC_TYPE_GET_OVERFLOW_ATABLE_ENTRY_RESP: {
    handle_get_overflow_atable_entry_resp(head_buf);
  } break;
  default: {
    // default_handle_recvd_result(res);
    info("switch default");
  } break;
  }
  // free(buf);
  //  for (auto v : res) {
  //    GlobalEnv::instance().mem_pool()->put_one_block(v);
  //  }
  GlobalEnv::rpc()->memory_pool()->put_one_block(head_blk);
  if (type != RpcMessageType::RPC_TYPE_READ_RESP) {
    GlobalEnv::block_pool()->free(data_blk.first);
  }
}

auto handle_find_req(void *buf) -> void{
    // auto *req = reinterpret_cast<RequestFind *>(buf);
    // char *real_buf_addr_ =
    //     reinterpret_cast<char *>((uintptr_t)buf + sizeof(RequestFind));
};
auto handle_stat_req(void *buf) -> void{
    // auto *req = reinterpret_cast<RequestStat *>(buf);
    // char *real_buf_addr_ =
    //     reinterpret_cast<char *>((uintptr_t)buf + sizeof(RequestStat));
};
auto handle_statfs_req(void *buf) -> void{
    // auto *req = reinterpret_cast<RequestStatfs *>(buf);
    // char *real_buf_addr_ =
    //     reinterpret_cast<char *>((uintptr_t)buf + sizeof(RequestStatfs));
};
auto handle_speed_req(void *buf) -> void{
    // auto *req = reinterpret_cast<RequestSpeed *>(buf);
    // char *real_buf_addr_ =
    //     reinterpret_cast<char *>((uintptr_t)buf + sizeof(RequestSpeed));
};
auto handle_sync_req(void *buf) -> void{
    // auto *req = reinterpret_cast<RpcFsyncReq *>(buf);
    // char *real_buf_addr_ =
    //     reinterpret_cast<char *>((uintptr_t)buf + sizeof(RpcFsyncReq));
    // req->resp_.set_value(RpcFsyncResp{true});
};
auto handle_open_req(void *buf) -> void{
    // auto *req = reinterpret_cast<RpcOpenReq *>(buf);
    // char *real_buf_addr_ =
    //     reinterpret_cast<char *>((uintptr_t)buf + sizeof(RpcOpenReq));
    // // need to query rocks db for ino of dir component and check the
    // permission

    // // should use consistent hash
    // auto key = std::to_string(req->p_ino_.ino_high_) +
    //            std::to_string(req->p_ino_.ino_low_) +
    //            req->get_pathname_conponent();
    // req->idx_of_path_++;
    // if (req->src_node_id_ == GlobalEnv::node_id()) {
    //   req->resp_pro_.set_value(req->resp_);
    // }
    // if (req->dir_num_ == req->idx_of_path_) {
    //   req->resp_.ino_ = 1;
    //   req->resp_.ok_ = true;
    //   GlobalEnv::rpc()node()->send_rpc_req(req->src_node_id_, req,
    //                                                  sizeof(RpcOpenReq));
    //   return;
    // }
    // auto dst_node_id = ConsistentHash::get_server_id(key);
    // // forward open request to next node
    // GlobalEnv::rpc()node()->send_rpc_req(dst_node_id, req,
    //                                                sizeof(RpcOpenReq));
};

auto handle_parse_path_req(void *buf) -> void {
  info("recv parse_path request");
  auto *req = reinterpret_cast<RequestParsePath *>(buf);
  GlobalEnv::metadata_handler()->handle_parse_path_req(req);
}

auto handle_parse_path_resp(void *buf) -> void {
  info("recv parse_path response");
  auto *resp = reinterpret_cast<ResponseParsePath *>(buf);
  info("resp msg : {}", (uint16_t)resp->msg_);
  //  notify
  memcpy(reinterpret_cast<void *>(resp->notify_.resp_addr_), resp,
         sizeof(ResponseParsePath));
  *reinterpret_cast<bool *>(resp->notify_.complete_addr_) = true;
  reinterpret_cast<FutexParker *>(resp->notify_.futex_addr_)->unpark();
}

auto handle_get_inode_req(void *buf) -> void {
  info("recv get_inode request");
  auto *req = reinterpret_cast<RequestGetInode *>(buf);
  GlobalEnv::metadata_handler()->handle_get_inode_req(req);
}

auto handle_get_inode_resp(void *head_buf, void *data_buf) -> void {
  info("recv get_inode response");
  auto *resp = reinterpret_cast<ResponseGetInode *>(head_buf);
  info("resp msg : {}", (uint16_t)resp->msg_);

  // notify
  *reinterpret_cast<ResponseGetInode *>(resp->notify_.resp_addr_) = *resp;
  if (resp->is_dir_) {
    memcpy(reinterpret_cast<void *>(resp->notify_.data_addr_), data_buf,
           DIR_INODE_SIZE);
  } else {
    memcpy(reinterpret_cast<void *>(resp->notify_.data_addr_), data_buf,
           FILE_INODE_SIZE);
  }

  *reinterpret_cast<bool *>(resp->notify_.complete_addr_) = true;
  reinterpret_cast<FutexParker *>(resp->notify_.futex_addr_)->unpark();
}

auto handle_add_dentry_req(void *buf) -> void {
  info("recv add_dentry request");
  auto *req = reinterpret_cast<RequestAddDentry *>(buf);
  GlobalEnv::metadata_handler()->handle_add_dentry_req(req);
}

auto handle_add_dentry_resp(void *buf) -> void {
  info("recv add_dentry response");
  auto *resp = reinterpret_cast<ResponseAddDentry *>(buf);
  info("resp msg : {}", (uint16_t)resp->msg_);

  // notify
  memcpy(reinterpret_cast<ResponseAddDentry *>(resp->notify_.resp_addr_), resp,
         sizeof(ResponseAddDentry));
  *reinterpret_cast<bool *>(resp->notify_.complete_addr_) = true;
  reinterpret_cast<FutexParker *>(resp->notify_.futex_addr_)->unpark();
}

auto handle_create_inode_req(void *buf) -> void {
  info("recv create_inode request");
  auto *req = reinterpret_cast<RequestCreateInode *>(buf);
  GlobalEnv::metadata_handler()->handle_create_inode_req(req);
}

auto handle_create_inode_resp(void *buf) -> void {
  info("recv create_inode response");
  auto *resp = reinterpret_cast<ResponseCreateInode *>(buf);
  info("resp msg : {}", (uint16_t)resp->msg_);

  // notify
  memcpy(reinterpret_cast<void *>(resp->notify_.resp_addr_), resp,
         sizeof(ResponseCreateInode));
  *reinterpret_cast<bool *>(resp->notify_.complete_addr_) = true;
  reinterpret_cast<FutexParker *>(resp->notify_.futex_addr_)->unpark();
}

auto handle_remove_dentry_req(void *buf) -> void {
  info("recv remove_dentry request");
  auto *req = reinterpret_cast<RequestRemoveDentry *>(buf);
  GlobalEnv::metadata_handler()->handle_remove_dentry_req(req);
}

auto handle_remove_dentry_resp(void *buf) -> void {
  info("recv remove_dentry response");
  auto *resp = reinterpret_cast<ResponseRemoveDentry *>(buf);
  info("resp msg : {}", (uint16_t)resp->msg_);

  // notify
  memcpy(reinterpret_cast<void *>(resp->notify_.resp_addr_), resp,
         sizeof(ResponseRemoveDentry));
  *reinterpret_cast<bool *>(resp->notify_.complete_addr_) = true;
  reinterpret_cast<FutexParker *>(resp->notify_.futex_addr_)->unpark();
}

auto handle_write_req(void *head_buf, void *data_buf) -> void {
  info("recv write request");
  auto *req = reinterpret_cast<RequestWrite *>(head_buf);
  GlobalEnv::metadata_handler()->handle_write_req(req, data_buf);
};

auto handle_write_resp(void *buf) -> void {
  info("recv write resp");
  auto *resp = reinterpret_cast<ResponseWrite *>(buf);
  info("resp msg : {}", (uint16_t)resp->msg_);

  // notify
  memcpy(reinterpret_cast<void *>(resp->notify_.resp_addr_), resp,
         sizeof(ResponseWrite));
  *reinterpret_cast<bool *>(resp->notify_.complete_addr_) = true;
  reinterpret_cast<FutexParker *>(resp->notify_.futex_addr_)->unpark();
}

auto handle_inline_write_req(void *head_buf, void *data_buf) -> void {
  info("recv inline write request");
  auto *req = reinterpret_cast<RequestInlineWrite *>(head_buf);
  GlobalEnv::metadata_handler()->handle_inline_write_req(req, data_buf);
}

auto handle_inline_write_resp(void *buf) -> void {
  info("recv inline write response");
  auto *resp = reinterpret_cast<ResponseInlineWrite *>(buf);
  info("resp msg : {}", (uint16_t)resp->msg_);

  // notify
  memcpy(reinterpret_cast<void *>(resp->notify_.resp_addr_), resp,
         sizeof(ResponseInlineWrite));
  *reinterpret_cast<bool *>(resp->notify_.complete_addr_) = true;
  reinterpret_cast<FutexParker *>(resp->notify_.futex_addr_)->unpark();
}

auto handle_inline_data_migrate_req(void *buf) -> void {
  info("recv inline data migrate request");
  auto *req = reinterpret_cast<RequestInlineDataMigrate *>(buf);
  GlobalEnv::metadata_handler()->handle_inline_data_migrate_req(req);
}

auto handle_inline_data_migrate_resp(void *buf, void *data_buf) -> void {
  info("recv inline data migrate response");
  auto *resp = reinterpret_cast<ResponseInlineDataMigrate *>(buf);
  info("resp msg : {}", (uint16_t)resp->msg_);

  // notify
  memcpy(reinterpret_cast<void *>(resp->notify_.resp_addr_), resp,
         sizeof(ResponseInlineDataMigrate));
  memcpy(reinterpret_cast<void *>(resp->notify_.data_addr_), data_buf,
         FILE_INODE_SIZE);
  *reinterpret_cast<bool *>(resp->notify_.complete_addr_) = true;
  reinterpret_cast<FutexParker *>(resp->notify_.futex_addr_)->unpark();
}

auto handle_update_inode_req(void *head_buf, void *data_buf) -> void {
  info("recv update_inode request");
  auto *req = reinterpret_cast<RequestUpdateInode *>(head_buf);

  GlobalEnv::metadata_handler()->handle_update_inode_req(req, data_buf);
}

auto handle_update_inode_resp(void *buf) -> void {
  info("recv update_inode resp");
  auto *resp = reinterpret_cast<ResponseUpdateInode *>(buf);
  info("resp msg : {}", (uint16_t)resp->msg_);

  // notify
  memcpy(reinterpret_cast<void *>(resp->notify_.resp_addr_), resp,
         sizeof(ResponseUpdateInode));
  *reinterpret_cast<bool *>(resp->notify_.complete_addr_) = true;
  reinterpret_cast<FutexParker *>(resp->notify_.futex_addr_)->unpark();
}

auto handle_read_req(void *buf) -> void {
  info("recv read request");
  auto *req = reinterpret_cast<RequestRead *>(buf);
  GlobalEnv::metadata_handler()->handle_read_req(req);
}

auto handle_read_resp(void *buf, std::pair<uint32_t, void *> data_blk) -> void {
  info("recv read resp");
  auto *resp = reinterpret_cast<ResponseRead *>(buf);
  info("resp msg : {}", (uint16_t)resp->msg_);

  // notify
  *reinterpret_cast<MetadataRespMsg *>(resp->notify_.msg_addr_) = resp->msg_;
  *reinterpret_cast<bool *>(resp->notify_.complete_addr_) = true;
  // memcpy(reinterpret_cast<void *>(resp->notify_.data_addr_), data_buf,
  //        GlobalEnv::config().block_size_);
  *reinterpret_cast<std::pair<uint32_t, void *> *>(resp->notify_.data_addr_) =
      data_blk;

  reinterpret_cast<FutexParker *>(resp->notify_.futex_addr_)->unpark();
}

auto handle_delete_dentry_req(void *buf) -> void {
  // info("recv delete dentry req");
  info("recv delete dentry req");
  auto *req = reinterpret_cast<RequestDeleteDentry *>(buf);
  GlobalEnv::metadata_handler()->handle_delete_dentry_req(req);
}

auto handle_delete_inode_req(void *buf) -> void {
  // info("recv delete inode req");
  info("recv delete inode req");
  auto *req = reinterpret_cast<RequestDeleteInode *>(buf);
  GlobalEnv::metadata_handler()->handle_delete_inode_req(req);
}

auto handle_delete_data_req(void *buf) -> void {
  // info("recv delete data req");
  info("recv delete data req");
  auto *req = reinterpret_cast<RequestDeleteData *>(buf);
  GlobalEnv::metadata_handler()->handle_delete_data_req(req);
}

auto handle_delete_inode_resp(void *buf) -> void {
  // info("recv delete inode resp");
  info("recv delete inode resp");
  auto *resp = reinterpret_cast<ResponseDeleteInode *>(buf);
  // info("resp msg : {}", (uint16_t)resp->msg_);
  info("resp msg : {}", (uint16_t)resp->msg_);

  // notify
  memcpy(reinterpret_cast<void *>(resp->notify_.resp_addr_), resp,
         sizeof(ResponseDeleteInode));
  *reinterpret_cast<bool *>(resp->notify_.complete_addr_) = true;
  reinterpret_cast<FutexParker *>(resp->notify_.futex_addr_)->unpark();
}

auto handle_delete_dentry_resp(void *buf) -> void {
  // info("recv delete inode resp");
  info("recv delete dentry resp");
  auto *resp = reinterpret_cast<ResponseDeleteDentry *>(buf);
  // info("resp msg : {}", (uint16_t)resp->msg_);
  info("resp msg : {}", (uint16_t)resp->msg_);

  // notify
  memcpy(reinterpret_cast<void *>(resp->notify_.resp_addr_), resp,
         sizeof(ResponseDeleteDentry));
  *reinterpret_cast<bool *>(resp->notify_.complete_addr_) = true;
  reinterpret_cast<FutexParker *>(resp->notify_.futex_addr_)->unpark();
}

auto handle_add_overflow_atable_entry_req(void *buf) -> void {
  info("recv add overflow atable entry req");
  auto *req = reinterpret_cast<RequestAddOverflowATableEntry *>(buf);
  GlobalEnv::metadata_handler()->handle_add_overflow_atable_entry_req(req);
}

auto handle_add_overflow_atable_entry_resp(void *buf) -> void {
  info("recv add overflow atable entry req");
  auto *resp = reinterpret_cast<ResponseAddOverflowATableEntry *>(buf);
  info("resp msg : {}", (uint16_t)resp->msg_);

  // notify
  memcpy(reinterpret_cast<void *>(resp->notify_.resp_addr_), resp,
         sizeof(ResponseAddOverflowATableEntry));
  *reinterpret_cast<bool *>(resp->notify_.complete_addr_) = true;
  reinterpret_cast<FutexParker *>(resp->notify_.futex_addr_)->unpark();
}

auto handle_get_overflow_atable_entry_req(void *buf) -> void {
  info("recv get overflow atable entry req");
  auto *req = reinterpret_cast<RequestGetOverflowATableEntry *>(buf);
  GlobalEnv::metadata_handler()->handle_get_overflow_atable_entry_req(req);
}

auto handle_get_overflow_atable_entry_resp(void *buf) -> void {
  info("recv get overflow atable entry resp");
  auto *resp = reinterpret_cast<ResponseGetOverflowATableEntry *>(buf);

  info("resp msg : {}", (uint16_t)resp->msg_);

  // notify
  memcpy(reinterpret_cast<void *>(resp->notify_.resp_addr_), resp,
         sizeof(ResponseGetOverflowATableEntry));
  *reinterpret_cast<bool *>(resp->notify_.complete_addr_) = true;
  reinterpret_cast<FutexParker *>(resp->notify_.futex_addr_)->unpark();
}

}; // namespace nextfs::rpc
