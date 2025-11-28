#include "daemon/ipc_receiver.hh"
#include "common/config.hh"
#include "common/ipc_message.hh"
#include "common/logger.hh"
#include "common/util.hh"
#include "daemon/daemon_file.hh"
#include "daemon/env.hh"
#include "iostream"
#include "metadata/args.hh"
#include "metadata/inode.hh"
#include "metadata/metadata.hh"
#include "rpc/message.hh"
#include "spdlog/fmt/fmt.h"
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <fcntl.h>
#include <string>
#include <sys/types.h>
#include <thread>
#include <vector>
namespace nextfs {
auto poll_ipc_message(IpcMessageInfo *infos, size_t n) -> size_t {
  return GlobalEnv::ipc_channel()->dequeue_bulk(infos, n);
}

auto wait_client_state(IpcMessageSync &sync, IpcState expected) -> void {
  while (sync.state_.load(std::memory_order_acquire) != expected) {
    // std::this_thre ad::yield();
    cpu_relax();
  }
}
auto get_file_dir(std::string full_path) -> std::string {
  uint16_t path_len = full_path.length();
  // int pos = full_path.rfind('/');
  // return full_path.substr(0, pos + 1);
  for (int i = path_len - 1; i > 0; i--) {
    if (full_path[i] == '/') {
      return full_path.substr(0, i);
    }
  }
  return "/";
}
auto get_file_name(std::string full_path) -> std::string {
  uint16_t path_len = full_path.length();
  // int pos = full_path.rfind('/');
  // return full_path.substr(pos + 1);
  for (int i = path_len; i >= 0; i--) {
    if (full_path[i] == '/') {
      return full_path.substr(i + 1);
    }
  }
  return nullptr;
}
auto ipc_msg_handler(IpcMessageInfo info) -> void {
  auto &daemon_env = GlobalEnv::instance();
  auto obj_pool = daemon_env.ipc_pool();
  auto obj = obj_pool->get<IpcMessage>(info.pool_index_);
  // auto msg = info;
  // (ipc_pool->get(pool_index).openatMsg).get_derive<IpcOpenatMsg>()
  trace("ipc_msg_handler {}", (int)info.type_);
  switch (info.type_) {
  case IpcMessageType::IPC_MSG_READ:
    ipc_handle_read(info, obj->sync_, obj->data_.readMsg);
    break;
  case IpcMessageType::IPC_MSG_WRITE:
    ipc_handle_write(info, obj->sync_, obj->data_.writeMsg);
    break;
  case IpcMessageType::IPC_MSG_MKDIR:
    ipc_handle_mkdir(info, obj->sync_, obj->data_.mkdirMSg);
    break;
  case IpcMessageType::IPC_MSG_OPEN:
    ipc_handle_open(info, obj->sync_, obj->data_.openatMsg);
    break;
  case IpcMessageType::IPC_MSG_CLOSE:
    ipc_handle_close(info, obj->sync_, obj->data_.closeMsg);
    break;
  case IpcMessageType::IPC_MSG_RENAME:
    ipc_handle_rename(info, obj->sync_, obj->data_.renameMsg);
    break;
  case IpcMessageType::IPC_MSG_STAT:
    ipc_handle_stat(info, obj->sync_, obj->data_.statMsg);
    break;
  case IpcMessageType::IPC_MSG_UNLINK:
    ipc_handle_unlink(info, obj->sync_, obj->data_.unlinkMsg);
    break;
  case IpcMessageType::IPC_MSG_FSYNC:
    obj->sync_.state_.store(IpcState::IPC_STATE_DAEMON_DONE,
                            std::memory_order_release);
    break;
  case IpcMessageType::IPC_MSG_NOP:
    obj->sync_.state_.store(IpcState::IPC_STATE_DAEMON_DONE,
                            std::memory_order_release);
    break;
  default:
    break;
  }
  trace("ipc_receiver over!");
}

void ipc_handle_mkdir(IpcMessageInfo info [[maybe_unused]],
                      IpcMessageSync &sync, IpcMkdiratMsg &msg) {
  trace("ipc receiver mkdir {}", msg.req_.path_string_);
  ParsePathResult parent_dir_result = GlobalEnv::metadata_handler()->parse_path(
      ParsePathArgs{get_file_dir(msg.req_.path_string_)});

  if (parent_dir_result.msg_ == ResultMsg::PARSE_PATH_SUCCESS) {
    trace("PARSE_PATH_SUCCESS");
    ino_type dir_ino = parent_dir_result.target_ino_;

    // 雪花算法生成ino
    ino_type create_dir_ino = Snowflake::next_ino();
    create_dir_ino.group_id_ = GlobalEnv::group_id();
    if (GlobalEnv::config().dir_store_policy_ == "local") {
      create_dir_ino.server_id_ = GlobalEnv::server_id();
    } else if (GlobalEnv::config().dir_store_policy_ == "hash") {
      string key =
          fmt::format("{}#{}#{}", create_dir_ino.ino_high_,
                      create_dir_ino.ino_low_, create_dir_ino.group_id_);
      create_dir_ino.server_id_ = ConsistentHash::get_server_id(key);
    } else {
      error("unknown dir store policy. store at local.");
      create_dir_ino.server_id_ = GlobalEnv::server_id();
    }

    CreateResult createResult =
        GlobalEnv::metadata_handler()->create(CreateArgs{
            .is_dir_ = true,
            .parent_ino_ = dir_ino,
            .ino_ = create_dir_ino,
            .file_name_ = get_file_name(msg.req_.path_string_),
        });
    if (createResult.msg_ == ResultMsg::CREATE_FAILED) {
      warn("Create error");
      msg.resp_.RespMsg = FileRespMsg::MKDIR_FAIL;
    } else {
      trace("Create success");
      msg.resp_.RespMsg = FileRespMsg::MKDIR_SUCCESS;
    }
  } else {
    msg.resp_.RespMsg = FileRespMsg::MKDIR_FAIL;
    trace("PARSE_PATH_FAILED");
  }
  sync.state_.store(IpcState::IPC_STATE_DAEMON_DONE, std::memory_order_release);
  // interceptor free msg obj
}

void ipc_handle_open(IpcMessageInfo info [[maybe_unused]], IpcMessageSync &sync,
                     IpcOpenatMsg &msg) {
  trace("handle open, ipc_pool req->path :{}", msg.req_.path_string_);
  msg.resp_.RespMsg = FileRespMsg::OPEN_FAIL;

  ParsePathResult parseResult = GlobalEnv::metadata_handler()->parse_path(
      ParsePathArgs{msg.req_.path_string_});

  if (parseResult.msg_ == ResultMsg::PARSE_PATH_SUCCESS) {
    trace("parseResult.msg_ == ResultMsg::PARSE_PATH_SUCCESS");
    // file/dir exist
    if (msg.req_.flags_ & O_CREAT && msg.req_.flags_ & O_EXCL) {
      msg.resp_.RespMsg = FileRespMsg::OPEN_FAIL_FILE_EXIST;
    } else {
      msg.resp_.ino = parseResult.target_ino_;
      GetInodeResult getResult;
      GlobalEnv::metadata_handler()->get_inode(
          GetInodeArgs{parseResult.target_ino_}, getResult);
      if (getResult.msg_ == ResultMsg::GET_INODE_SUCCESS) {
        msg.resp_.RespMsg = FileRespMsg::OPEN_SUCCESS_WITHOUT_CREATE;
        std::shared_ptr<DaemonFileInfo> ret =
            GlobalEnv::open_daemon_file_manager().open(parseResult.target_ino_);
        ret->update_inode(getResult.file_inode_);
      }
    }
  } else if (parseResult.msg_ == ResultMsg::PARSE_PATH_FAILED_BUT_FIND_PARENT &&
             msg.req_.flags_ & O_CREAT) {
    // 获得父目录ino
    trace("file_dir {}", get_file_dir(msg.req_.path_string_));
    ParsePathResult parent_dir_result =
        GlobalEnv::metadata_handler()->parse_path(
            ParsePathArgs{get_file_dir(msg.req_.path_string_)});
    if (parent_dir_result.msg_ == ResultMsg::PARSE_PATH_SUCCESS) {
      ino_type dir_ino = parent_dir_result.target_ino_;
      // 雪花算法生成ino
      ino_type create_file_ino = Snowflake::next_ino();
      create_file_ino.group_id_ = GlobalEnv::group_id();
      if (GlobalEnv::config().file_store_policy_ == "local") {
        create_file_ino.server_id_ = GlobalEnv::server_id();
      } else if (GlobalEnv::config().file_store_policy_ == "hash") {
        string key =
            fmt::format("{}#{}#{}", create_file_ino.ino_high_,
                        create_file_ino.ino_low_, create_file_ino.group_id_);
        create_file_ino.server_id_ = ConsistentHash::get_server_id(key);
      } else {
        error("unknown file store policy. store at local.");
        create_file_ino.server_id_ = GlobalEnv::server_id();
      }
      // string key =
      //     fmt::format("{}#{}#{}", create_file_ino.ino_high_,
      //                 create_file_ino.ino_low_, create_file_ino.group_id_);
      // create_file_ino.server_id_ = ConsistentHash::get_server_id(key);
      // 创建文件
      CreateResult createResult =
          GlobalEnv::metadata_handler()->create(CreateArgs{
              .is_dir_ = false,
              .parent_ino_ = dir_ino,
              .ino_ = create_file_ino,
              .file_name_ = get_file_name(msg.req_.path_string_),
          });
      if (createResult.msg_ == ResultMsg::CREATE_SUCCESS) {
        trace("createResult.msg_ == ResultMsg::CREATE_SUCCESS");
        // OPEN
        GlobalEnv::open_daemon_file_manager().open(create_file_ino);
        msg.resp_.ino = create_file_ino;
        msg.resp_.RespMsg = FileRespMsg::OPEN_SUCCESS_WITH_CREATE;
      } else if (createResult.msg_ == ResultMsg::FILE_ALREADY_EXIST) {
        trace("createResult.msg_ == ResultMsg::FILE_ALREADY_EXIST");
        if (msg.req_.flags_ & O_CREAT && msg.req_.flags_ & O_EXCL) {
          msg.resp_.RespMsg = FileRespMsg::OPEN_FAIL_FILE_EXIST;
        } else {
          msg.resp_.ino = createResult.exist_ino_;
          GetInodeResult getResult;
          GlobalEnv::metadata_handler()->get_inode(
              GetInodeArgs{createResult.exist_ino_}, getResult);
          if (getResult.msg_ == ResultMsg::GET_INODE_SUCCESS) {
            msg.resp_.RespMsg = FileRespMsg::OPEN_SUCCESS_WITHOUT_CREATE;
            std::shared_ptr<DaemonFileInfo> ret =
                GlobalEnv::open_daemon_file_manager().open(
                    createResult.exist_ino_);
            ret->update_inode(getResult.file_inode_);
          }
        }
      } else {
        trace("createResult.msg_ == {}", (uint32_t)createResult.msg_);
      }
    }
  } else {
    msg.resp_.RespMsg = FileRespMsg::OPEN_FAIL_FILE_NOT_EXIST;
  }
  trace("ipc_handle_open() ipc sync");
  sync.state_.store(IpcState::IPC_STATE_DAEMON_DONE, std::memory_order_release);
}

void ipc_handle_close(IpcMessageInfo info [[maybe_unused]],
                      IpcMessageSync &sync, IpcCloseMsg &msg) {
  std::shared_ptr<DaemonFileInfo> ret =
      GlobalEnv::open_daemon_file_manager().get(msg.req_.file_ino);
  if (ret == nullptr) {
    msg.resp_.RespMsg = FileRespMsg::FILE_NOT_OPENED;
  } else {
    if (GlobalEnv::open_daemon_file_manager().close(msg.req_.file_ino)) {
      trace("close daemon file!");
      msg.resp_.RespMsg = FileRespMsg::CLOSE_DAEMON_SUCCESS;
      if (GlobalEnv::config().close_to_open_) {
        auto inode_rg = DaemonFileInfo::read(std::move(ret));
        UpdateInodeArgs update_args;
        update_args.file_ino_ = msg.req_.file_ino;
        update_args.file_size_ = inode_rg->file_size_;
        update_args.blocks_ = inode_rg.pending_blocks();
        auto res = GlobalEnv::metadata_handler()->update_inode(update_args);
        if (res.msg_ != ResultMsg::UPDATE_INODE_SUCCESS) {
          msg.resp_.RespMsg = FileRespMsg::WRITE_FAIL;
        }
      }
    } else {
      trace("add_finterceptor_file_num - 1");
      msg.resp_.RespMsg = FileRespMsg::CLOSE_HOOK_FILE;
    }
  }
  sync.state_.store(IpcState::IPC_STATE_DAEMON_DONE, std::memory_order_release);
}

void ipc_handle_rename(IpcMessageInfo info [[maybe_unused]],
                       IpcMessageSync &sync, IpcRenameMsg &msg) {
  trace("ipc receiver rename {} to {}", msg.req_.old_pathname_,
        msg.req_.new_pathname_);
  msg.resp_.RespMsg = FileRespMsg::RENAME_FAIL;
  ParsePathResult old_parent_dir_result =
      GlobalEnv::metadata_handler()->parse_path(
          ParsePathArgs{get_file_dir(msg.req_.old_pathname_)});
  if (old_parent_dir_result.msg_ != ResultMsg::PARSE_PATH_SUCCESS) {
    warn("parse old file dir fail!");
    sync.state_.store(IpcState::IPC_STATE_DAEMON_DONE,
                      std::memory_order_release);
    return;
  }

  ParsePathResult new_parent_dir_result =
      GlobalEnv::metadata_handler()->parse_path(
          ParsePathArgs{get_file_dir(msg.req_.new_pathname_)});
  if (new_parent_dir_result.msg_ != ResultMsg::PARSE_PATH_SUCCESS) {
    warn("parse new file dir fail!");
    sync.state_.store(IpcState::IPC_STATE_DAEMON_DONE,
                      std::memory_order_release);
    return;
  }

  RenameArgs renameargs;
  renameargs.old_parent_ino_ = old_parent_dir_result.target_ino_;
  renameargs.new_parent_ino_ = new_parent_dir_result.target_ino_;
  renameargs.old_name_ = get_file_name(msg.req_.old_pathname_);
  renameargs.new_name_ = get_file_name(msg.req_.new_pathname_);
  RenameResult result = GlobalEnv::metadata_handler()->rename(renameargs);
  if (result.msg_ == ResultMsg::RENAME_SUCCESS) {
    msg.resp_.RespMsg = FileRespMsg::RENAME_SUCCESS;
  }
  sync.state_.store(IpcState::IPC_STATE_DAEMON_DONE, std::memory_order_release);
}

void ipc_handle_stat(IpcMessageInfo info [[maybe_unused]], IpcMessageSync &sync,
                     IpcStatMsg &msg) {
  trace("ipc receiver stat {}", msg.req_.path_string_);
  msg.resp_.RespMsg = FileRespMsg::STAT_FAIL;
  ino_type target_ino = msg.req_.ino_;
  if (!msg.req_.use_ino_) {
    ParsePathResult parseResult = GlobalEnv::metadata_handler()->parse_path(
        ParsePathArgs{msg.req_.path_string_});
    if (parseResult.msg_ == ResultMsg::PARSE_PATH_SUCCESS) {
      target_ino = parseResult.target_ino_;
    } else {
      trace("file not exist");
      sync.state_.store(IpcState::IPC_STATE_DAEMON_DONE,
                        std::memory_order_release);
      return;
    }
  }

  GetInodeResult getResult;
  GlobalEnv::metadata_handler()->get_inode(GetInodeArgs{target_ino}, getResult);
  if (getResult.msg_ == ResultMsg::GET_INODE_SUCCESS) {
    msg.resp_.statMessage.ino = target_ino;
    msg.resp_.statMessage.perm_ = getResult.file_inode_.perm_;
    msg.resp_.statMessage.create_time_ = getResult.file_inode_.create_time_;
    msg.resp_.statMessage.access_time_ = getResult.file_inode_.access_time_;
    msg.resp_.statMessage.modify_time_ = getResult.file_inode_.modify_time_;
    msg.resp_.statMessage.file_size_ = getResult.file_inode_.file_size_;
    msg.resp_.RespMsg = FileRespMsg::STAT_SUCCESS;
    if (auto fh = GlobalEnv::open_daemon_file_manager().get(target_ino);
        fh != nullptr) {
      fh->update_inode(getResult.file_inode_);
    }
  }
  sync.state_.store(IpcState::IPC_STATE_DAEMON_DONE, std::memory_order_release);
}

void ipc_handle_unlink(IpcMessageInfo info [[maybe_unused]],
                       IpcMessageSync &sync, IpcUnlinkMsg &msg) {
  msg.resp_.RespMsg = FileRespMsg::UNLINK_FAIL;
  // 父目录路径解析
  ParsePathResult parent_dir_result = GlobalEnv::metadata_handler()->parse_path(
      ParsePathArgs{get_file_dir(msg.req_.path_string_)});
  if (parent_dir_result.msg_ == ResultMsg::PARSE_PATH_SUCCESS) {
    ino_type dir_ino = parent_dir_result.target_ino_;

    DeleteArgs delete_args;
    delete_args.file_name_ = get_file_name(msg.req_.path_string_);
    delete_args.parent_ino_ = dir_ino;
    DeleteResult delete_ret =
        GlobalEnv::metadata_handler()->delete_file(delete_args);
    msg.resp_.RespMsg = delete_ret.msg_ == ResultMsg::DELETE_SUCCESS
                            ? FileRespMsg::UNLINK_SUCCESS
                            : FileRespMsg::UNLINK_FAIL;
  }
  sync.state_.store(IpcState::IPC_STATE_DAEMON_DONE, std::memory_order_release);
}

void ipc_handle_read(IpcMessageInfo info, IpcMessageSync &sync,
                     IpcReadMsg &msg) {
  auto fi = GlobalEnv::open_daemon_file_manager().get(msg.req_.ino_);
  std::vector<uint32_t> allocated_blocks;
  auto finish_read = [&]() {
    wait_client_state(sync, IpcState::IPC_STATE_CLIENT_DONE);
    GlobalEnv::ipc_pool()->free(info.pool_index_);
    for (auto bidx : allocated_blocks) {
      GlobalEnv::block_pool()->free(bidx);
    }
  };
  if (fi == nullptr) {
    msg.resp_.RespMsg = FileRespMsg::READ_FAIL;
    msg.resp_.block_num_ = 0;
    sync.state_.store(IpcState::IPC_STATE_DAEMON_DONE,
                      std::memory_order_release);
    finish_read();
    return;
  }
  auto block_pool = GlobalEnv::block_pool();
  auto inode_rg = DaemonFileInfo::read(std::move(fi));
  // TODO: we dont have method to protect distributed inode read
  // TODO: need to update inode to read newest inode

  // 1. small file fast path
  if (!inode_rg->has_atable_) {
    if (msg.req_.offset_ >= inode_rg->file_size_) {
      msg.resp_.RespMsg = FileRespMsg::READ_SUCCESS;
      msg.resp_.block_num_ = 0;
    } else {
      auto [bidx, addr] = block_pool->alloc<void>().value();
      allocated_blocks.push_back(bidx);
      auto read_size =
          std::min(msg.req_.count_, inode_rg->file_size_ - msg.req_.offset_);
      memcpy(addr, inode_rg->inline_data_ + msg.req_.offset_, read_size);
      msg.resp_.RespMsg = FileRespMsg::READ_SUCCESS;
      msg.resp_.block_num_ = 1;
      msg.resp_.blocks_[0] = {bidx, static_cast<uint32_t>(read_size)};
    }
    inode_rg.unlock();
    sync.state_.store(IpcState::IPC_STATE_DAEMON_DONE,
                      std::memory_order_release);
    finish_read();
    return;
  }

  // 2. file is not a inline file. get each block's target node id
  struct BlockReq {
    uint32_t bno_;
    uint32_t offset_;
    uint32_t size_;
    uint32_t target_node_id_{0};
  };
  std::vector<BlockReq> reqs;
  for (uint64_t pos = msg.req_.offset_;
       pos < msg.req_.offset_ + msg.req_.count_ &&
       pos < inode_rg->file_size_;) {
    BlockReq req;
    req.bno_ = pos / GlobalEnv::config().block_size_;
    req.offset_ = pos % GlobalEnv::config().block_size_;
    req.size_ = std::min(
        msg.req_.count_ - (pos - msg.req_.offset_),
        static_cast<uint64_t>(GlobalEnv::config().block_size_) - req.offset_);
    req.size_ =
        std::min(static_cast<uint64_t>(req.size_), inode_rg->file_size_ - pos);
    pos += req.size_;
    // get target node id
    if (req.bno_ < max_block_count) {
      req.target_node_id_ = inode_rg->cast_ATable()->map_[req.bno_];
    }
    reqs.push_back(req);
  }
  inode_rg.unlock();
  //! we cannot use inode_rg after this line

  for (auto &req : reqs) {
    // first read local BTable
    if (req.target_node_id_ == 0) {
      btable_item blk_info;
      auto s = GlobalEnv::metadata_handler()->get_btable_db()->get_block_info(
          btable_key{msg.req_.ino_, req.bno_}, &blk_info);
      if (s.ok()) {
        req.target_node_id_ = GlobalEnv::node_id(); // block store in local
      } else {
        btable_item blk_info;
        auto s = GlobalEnv::metadata_handler()->get_btable_db()->get_block_info(
            btable_key{msg.req_.ino_, req.bno_}, &blk_info);
        if (s.ok()) {
          req.target_node_id_ = GlobalEnv::node_id(); // block store in local
        } else {
          // read global overflow ATable
          // TODO: concurrent get overflow ATable
          overflow_atable_item overflow_blk_info;
          GetOverflowATableEntryArgs args;
          args.file_ino_ = msg.req_.ino_;
          args.block_num_ = req.bno_;
          auto res =
              GlobalEnv::metadata_handler()->get_overflow_atable_entry(args);
          if (res.msg_ == ResultMsg::GET_OVERFLOW_ATABLE_ENTRY_SUCCESS) {
            req.target_node_id_ = res.node_id_;
          } else {
            msg.resp_.RespMsg = FileRespMsg::READ_FAIL;
            msg.resp_.block_num_ = 0;
            sync.state_.store(IpcState::IPC_STATE_DAEMON_DONE,
                              std::memory_order_release);
            finish_read();
            return;
          }
        }
      }
    }
  }

  // 3. read blocks
  // TODO: concurrent read
  // auto [bidx1, addr1] = block_pool->alloc<void>().value();
  // auto [bidx2, addr2] = block_pool->alloc<void>().value();
  // allocated_blocks.push_back(bidx1);
  // allocated_blocks.push_back(bidx2);
  // std::pair<uint32_t, void *> ready_blk = {bidx1, addr1};
  // std::pair<uint32_t, void *> pending_blk = {bidx2, addr2};

  for (auto &br : reqs) {
    ReadArgs args;
    args.file_ino_ = msg.req_.ino_;
    args.node_id_ = br.target_node_id_;
    args.block_num_ = br.bno_;
    args.offset_ = br.offset_;
    args.read_size_ = br.size_;

    // args.data_ = ready_blk.second; // TODO: avoid copy
    auto res = GlobalEnv::metadata_handler()->read(args);
    allocated_blocks.push_back(res.data_blk_.first);
    if (res.msg_ != ResultMsg::READ_SUCCESS) {
      msg.resp_.RespMsg = FileRespMsg::READ_FAIL;
      msg.resp_.block_num_ = 0;
      sync.state_.store(IpcState::IPC_STATE_DAEMON_DONE,
                        std::memory_order_release);
      finish_read();
      return;
    }

    wait_client_state(sync, IpcState::IPC_STATE_PENDING);
    msg.resp_.RespMsg = FileRespMsg::READ_SUCCESS;
    msg.resp_.block_num_ = 1;
    msg.resp_.blocks_[0] = {res.data_blk_.first, br.size_};
    sync.state_.store(br.bno_ == reqs.back().bno_
                          ? IpcState::IPC_STATE_DAEMON_DONE
                          : IpcState::IPC_STATE_RESP_SEND,
                      std::memory_order_release);
    // std::swap(ready_blk, pending_blk);
  }
  if (reqs.empty()) {
    msg.resp_.RespMsg = FileRespMsg::READ_SUCCESS;
    msg.resp_.block_num_ = 0;
    sync.state_.store(IpcState::IPC_STATE_DAEMON_DONE,
                      std::memory_order_release);
  }
  finish_read();
}

void ipc_handle_write(IpcMessageInfo info, IpcMessageSync &sync,
                      IpcWriteMsg &msg) {
  // auto t1 = std::chrono::high_resolution_clock::now();
  size_t max_block_to_write =
      msg.req_.count_ / GlobalEnv::config().block_size_ + 1;
  auto fi = GlobalEnv::open_daemon_file_manager().get(msg.req_.ino_);
  std::vector<uint32_t> allocated_blocks;
  allocated_blocks.reserve(max_block_to_write);
  auto finish_write = [&]() {
    wait_client_state(sync, IpcState::IPC_STATE_CLIENT_DONE);
    GlobalEnv::ipc_pool()->free(info.pool_index_);
    for (auto bidx : allocated_blocks) {
      GlobalEnv::block_pool()->free(bidx);
    }
  };
  if (fi == nullptr) {
    msg.resp_.RespMsg = FileRespMsg::WRITE_FAIL;
    msg.resp_.block_num_ = 0;
    sync.state_.store(IpcState::IPC_STATE_DAEMON_DONE,
                      std::memory_order_release);
    finish_write();
    return;
  }
  bool inline_write =
      msg.req_.offset_ + msg.req_.count_ <= inline_data_size_limit;
  auto block_pool = GlobalEnv::block_pool();
  auto inode_rg = DaemonFileInfo::read(fi);
  auto file_size = inode_rg->file_size_;
  // 1. inline write fast path
  if (!inode_rg->has_atable_ && inline_write) {
    inode_rg.unlock();
    auto [bidx, addr] = block_pool->alloc<void>().value();
    allocated_blocks.push_back(bidx);
    msg.resp_.RespMsg = FileRespMsg::WRITE_SUCCESS;
    msg.resp_.block_num_ = 1;
    msg.resp_.blocks_[0] = {bidx, static_cast<uint32_t>(msg.req_.count_)};
    sync.state_.store(IpcState::IPC_STATE_RESP_SEND, std::memory_order_release);
    wait_client_state(sync, IpcState::IPC_STATE_PENDING);
    auto inode_wg = DaemonFileInfo::write(fi);
    if (inode_wg->has_atable_ == true) {
      // we dont allow concurrent write to same block as we dont have
      // distributed lock
      // TODO: fix here
      throw std::runtime_error("concurrent write to same block");
    }
    // issue write to metadata
    InlineWriteArgs args;
    args.file_ino_ = msg.req_.ino_;
    args.offset_ = msg.req_.offset_;
    args.data_size_ = msg.req_.count_;
    args.data_ = static_cast<char *>(addr);
    auto res = GlobalEnv::metadata_handler()->inline_write(args);
    if (res.msg_ == ResultMsg::INLINE_WRITE_FAILED) {
      inode_wg.unlock();
      msg.resp_.RespMsg = FileRespMsg::WRITE_FAIL;
    } else {
      memcpy(inode_wg->inline_data_ + msg.req_.offset_, addr, res.write_size_);
      inode_wg->file_size_ = res.inline_data_size_;
      inode_wg.unlock();
      msg.resp_.RespMsg = FileRespMsg::WRITE_SUCCESS;
    }
    msg.resp_.block_num_ = 0;
    sync.state_.store(IpcState::IPC_STATE_DAEMON_DONE,
                      std::memory_order_release);
    finish_write();
    return;
  }
  // 2. small file, but write overflow
  if (!inode_rg->has_atable_ && !inline_write) {
    // migrate
    inode_rg.unlock();
    {
      auto inode_wg = DaemonFileInfo::write(fi);
      if (inode_wg->has_atable_ != true) {
        // we dont allow concurrent write to same block as we dont have
        // distributed lock
        // throw std::runtime_error("concurrent write to same block");
        InlineDataMigrateResult res;
        GlobalEnv::metadata_handler()->inline_data_migrate(
            {.file_ino_ = msg.req_.ino_}, res);
        if (res.msg_ == ResultMsg::INLINE_DATA_MIGRATE_FAILED) {
          inode_wg.unlock();
          msg.resp_.RespMsg = FileRespMsg::WRITE_FAIL;
          msg.resp_.block_num_ = 0;
          sync.state_.store(IpcState::IPC_STATE_DAEMON_DONE,
                            std::memory_order_release);
          finish_write();
          return;
        }
        inode_wg.update_inode(res.file_inode_);
      }
    }
    inode_rg.lock();
  }
  // 3. file is not a inline file. get each block's target node id
  struct BlockReq {
    uint32_t bno_;
    uint32_t offset_;
    uint32_t size_;
    uint32_t target_node_id_{0};
    uint32_t bidx_; // block index in pool
    bool add_overflow_{false};
    void *block_data_;
  };
  std::vector<BlockReq> reqs;
  reqs.reserve(max_block_to_write);
  for (uint64_t pos = msg.req_.offset_;
       pos < msg.req_.offset_ + msg.req_.count_;) {
    BlockReq req;
    req.bno_ = pos / GlobalEnv::config().block_size_;
    req.offset_ = pos % GlobalEnv::config().block_size_;
    req.size_ = std::min(
        msg.req_.count_ - (pos - msg.req_.offset_),
        static_cast<uint64_t>(GlobalEnv::config().block_size_) - req.offset_);
    pos += req.size_;
    // get target node id
    if (req.bno_ < max_block_count) {
      req.target_node_id_ = inode_rg->cast_ATable()->map_[req.bno_];
    }
    auto [bidx, addr] = block_pool->alloc<void>().value();
    allocated_blocks.push_back(bidx);
    req.bidx_ = bidx;
    req.block_data_ = addr;
    reqs.push_back(req);
  }
  inode_rg.unlock();
  size_t max_block_num = sizeof(msg.resp_.blocks_) / sizeof(Block);
  for (int i = 0; i < reqs.size(); i++) {
    if (i % max_block_num == 0)
      wait_client_state(sync, IpcState::IPC_STATE_PENDING);
    msg.resp_.blocks_[i % max_block_num].idx_in_pool_ = reqs[i].bidx_;
    msg.resp_.blocks_[i % max_block_num].len_ = reqs[i].size_;
    if (i % max_block_num == max_block_num - 1 || i == reqs.size() - 1) {
      msg.resp_.RespMsg = FileRespMsg::WRITE_SUCCESS;
      msg.resp_.block_num_ = i % max_block_num + 1;
      sync.state_.store(IpcState::IPC_STATE_RESP_SEND,
                        std::memory_order_release);
    }
  }
  // auto t2 = std::chrono::high_resolution_clock::now();
  bool new_inline_block = false;
  for (auto &req : reqs) {
    if (req.target_node_id_ == 0) {
      // we cannot find target node id in inline ATable
      if (req.bno_ < max_block_count) {
        // if bno_ < inline_max, which mean block is not exist. store local.
        req.target_node_id_ = GlobalEnv::node_id();
        new_inline_block = true;
      } else {
        // read local BTable
        btable_item blk_info;
        auto s = GlobalEnv::metadata_handler()->get_btable_db()->get_block_info(
            btable_key{msg.req_.ino_, req.bno_}, &blk_info);
        if (s.ok()) {
          req.target_node_id_ = GlobalEnv::node_id(); // block store in local
        } else {
          // read global overflow ATable
          // TODO: concurrent get overflow ATable
          overflow_atable_item overflow_blk_info;
          GetOverflowATableEntryArgs args;
          args.file_ino_ = msg.req_.ino_;
          args.block_num_ = req.bno_;
          auto res =
              GlobalEnv::metadata_handler()->get_overflow_atable_entry(args);
          if (res.msg_ == ResultMsg::GET_OVERFLOW_ATABLE_ENTRY_SUCCESS) {
            req.target_node_id_ = res.node_id_;
          } else {
            // block doesnt exist, store local and add overflow ATable
            req.add_overflow_ = true;
            req.target_node_id_ = GlobalEnv::node_id();
          }
        }
      }
    }
  }

  // auto t3 = std::chrono::high_resolution_clock::now();
  wait_client_state(sync, IpcState::IPC_STATE_PENDING);
  // all data blocks ready
  // 4. write blocks
  WriteArgs write_reqs[reqs.size()];
  for (size_t i = 0; i < reqs.size(); i++) {
    auto &req = reqs[i];
    WriteArgs args;
    args.file_ino_ = msg.req_.ino_;
    args.data_node_id_ = req.target_node_id_;
    args.block_num_ = req.bno_;
    args.offset_ = req.offset_;
    args.data_size_ = req.size_;
    args.data_ = static_cast<char *>(req.block_data_);
    write_reqs[i] = args;
  }
  auto res =
      reqs.size() == 1
          ? GlobalEnv::metadata_handler()->write(write_reqs[0])
          : GlobalEnv::metadata_handler()->write_vec(write_reqs, reqs.size());
  if (res.msg_ != ResultMsg::WRITE_SUCCESS) {
    // TODO: handle error, atomic write?
    msg.resp_.RespMsg = FileRespMsg::WRITE_FAIL;
    msg.resp_.block_num_ = 0;
    sync.state_.store(IpcState::IPC_STATE_DAEMON_DONE,
                      std::memory_order_release);
    finish_write();
    return;
  }
  // auto t4 = std::chrono::high_resolution_clock::now();
  // std::chrono::system_clock::time_point t5, t6;
  // 5. update ATable
  {
    UpdateInodeArgs update_args;
    update_args.file_ino_ = msg.req_.ino_;
    update_args.file_size_ =
        std::max(file_size, msg.req_.offset_ + msg.req_.count_);

    for (auto &br : reqs) {
      if (br.bno_ < max_block_count && new_inline_block) {
        update_args.blocks_.emplace_back(br.bno_, br.target_node_id_);
      } else if (br.add_overflow_) {
        AddOverflowATableEntryArgs args;
        args.file_ino_ = msg.req_.ino_;
        args.block_num_ = br.bno_;
        args.node_id_ = br.target_node_id_;
        auto res =
            GlobalEnv::metadata_handler()->add_overflow_atable_entry(args);
        if (res.msg_ != ResultMsg::ADD_OVERFLOW_ATABLE_ENTRY_SUCCESS) {
          // TODO: handle error, atomic write?
          msg.resp_.RespMsg = FileRespMsg::WRITE_FAIL;
          msg.resp_.block_num_ = 0;
          sync.state_.store(IpcState::IPC_STATE_DAEMON_DONE,
                            std::memory_order_release);
          finish_write();
          return;
        }
      }
    }
    // t5 = std::chrono::high_resolution_clock::now();
    if (!GlobalEnv::config().close_to_open_) {
      auto res = GlobalEnv::metadata_handler()->update_inode(update_args);
      if (res.msg_ != ResultMsg::UPDATE_INODE_SUCCESS) {
        // TODO: handle error, atomic write?
        msg.resp_.RespMsg = FileRespMsg::WRITE_FAIL;
        msg.resp_.block_num_ = 0;
        sync.state_.store(IpcState::IPC_STATE_DAEMON_DONE,
                          std::memory_order_release);
        finish_write();
        return;
      }
    }

    // 6. update inode
    auto inode_wg = DaemonFileInfo::write(fi);
    inode_wg->file_size_ =
        std::max(update_args.file_size_, inode_wg->file_size_);
    inode_wg->modify_time_ = GlobalEnv::metadata_handler()->get_timestamp();
    for (auto &b : update_args.blocks_) {
      inode_wg->cast_ATable()->map_[b.first] = b.second;
    }
    if (GlobalEnv::config().close_to_open_) {
      inode_wg.update_pending_blocks(update_args.blocks_);
    }
    // t6 = std::chrono::high_resolution_clock::now();
  }
  msg.resp_.RespMsg = FileRespMsg::WRITE_SUCCESS;
  msg.resp_.block_num_ = 0;
  sync.state_.store(IpcState::IPC_STATE_DAEMON_DONE, std::memory_order_release);
  finish_write();
  // thread_local std::vector<uint64_t> times(5, 0);
  // thread_local uint64_t counter = 0;
  // counter++;
  // times[0] +=
  //     std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
  // times[1] +=
  //     std::chrono::duration_cast<std::chrono::microseconds>(t3 - t2).count();
  // times[2] +=
  //     std::chrono::duration_cast<std::chrono::microseconds>(t4 - t3).count();
  // times[3] +=
  //     std::chrono::duration_cast<std::chrono::microseconds>(t5 - t4).count();
  // times[4] +=
  //     std::chrono::duration_cast<std::chrono::microseconds>(t6 - t5).count();
  // if (counter != 0 && counter % 10000 == 0) {
  //   warn("write time: {} {} {} {} {}", times[0] / 10000, times[1] / 10000,
  //        times[2] / 10000, times[3] / 10000, times[4] / 10000);
  //   times = std::vector<uint64_t>(5, 0);
  // }
}

} // namespace nextfs
