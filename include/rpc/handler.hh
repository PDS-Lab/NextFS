#pragma once
#include "common/memory_pool.hh"
#include "rpc/connection.hh"
#include "rpc/message.hh"
namespace nextfs::rpc {

struct RpcHandleArg {
  BlockInfo *head_blk;
  std::pair<uint32_t, void *> data_blk;
};

auto handle_recvd_result(RpcHandleArg arg) -> void;
auto handle_read_req(void *buf) -> void;
auto handle_sync_req(void *buf) -> void;
auto handle_find_req(void *buf) -> void;
auto handle_stat_req(void *buf) -> void;
auto handle_statfs_req(void *buf) -> void;
auto handle_speed_req(void *buf) -> void;

// Metadata related
auto handle_parse_path_req(void *buf) -> void;
auto handle_parse_path_resp(void *buf) -> void;
auto handle_get_inode_req(void *buf) -> void;
auto handle_get_inode_resp(void *head_buf, void *data_buf) -> void;
auto handle_add_dentry_req(void *buf) -> void;
auto handle_add_dentry_resp(void *buf) -> void;
auto handle_create_inode_req(void *buf) -> void;
auto handle_create_inode_resp(void *buf) -> void;
auto handle_remove_dentry_req(void *buf) -> void;
auto handle_remove_dentry_resp(void *buf) -> void;
auto handle_write_req(void *head_buf, void *data_buf) -> void;
auto handle_write_resp(void *buf) -> void;
auto handle_inline_write_req(void *head_buf, void *data_buf) -> void;
auto handle_inline_write_resp(void *buf) -> void;
auto handle_inline_data_migrate_req(void *buf) -> void;
auto handle_inline_data_migrate_resp(void *buf, void *data_buf) -> void;
auto handle_update_inode_req(void *head_buf, void *data_buf) -> void;
auto handle_update_inode_resp(void *buf) -> void;
auto handle_read_req(void *buf) -> void;
auto handle_read_resp(void *buf, std::pair<uint32_t, void *> data_blk) -> void;
auto handle_delete_dentry_req(void *buf) -> void;
auto handle_delete_inode_req(void *buf) -> void;
auto handle_delete_data_req(void *buf) -> void;
auto handle_delete_inode_resp(void *buf) -> void;
auto handle_delete_dentry_resp(void *buf) -> void;
auto handle_add_overflow_atable_entry_req(void *buf) -> void;
auto handle_add_overflow_atable_entry_resp(void *buf) -> void;
auto handle_get_overflow_atable_entry_req(void *buf) -> void;
auto handle_get_overflow_atable_entry_resp(void *buf) -> void;

}; // namespace nextfs::rpc