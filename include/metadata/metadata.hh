#pragma once
#include "common/consistent_hash.hh"
#include "common/util.hh"
#include "daemon/env.hh"
#include "metadata/args.hh"
#include "metadata/btable_db.hh"
#include "metadata/data_block_mgr.hh"
#include "metadata/dentry_db.hh"
#include "metadata/inode.hh"
#include "metadata/meta_db.hh"
#include "metadata/overflow_atable_db.hh"
#include "metadata/util/idx_free_list.hh"
#include "metadata/util/snowflake.hh"
#include "rpc/message.hh"
#include "rpc/server.hh"
#include "string"
#include "vector"

#include "iostream"
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string_view>
#include <sys/types.h>
#include <tuple>
#include <utility>

namespace nextfs {

using std::string;
const int dent_locks_cnt = 64;
const int ino_locks_cnt = 64;

class MetadataHandler {
public:
  MetadataHandler(string dentry_db_path, Options dentry_db_options,
                  string meta_db_path, Options meta_db_options,
                  string btable_db_path, Options btable_db_options,
                  string overflow_atable_db_path,
                  Options overflow_atable_db_options);
  ~MetadataHandler();

  /*
    utils
  */
  auto rpc_send_blk(uint32_t node_id, BlockInfo *blk, size_t length) -> void;
  auto rpc_send_with_data(uint32_t node_id, BlockInfo *head_blk,
                          std::pair<uint32_t, void *> data_blk,
                          size_t data_size) -> void;

  auto hash_server_id(ino_type ino) -> uint32_t;
  auto hash_write_node(ino_type, uint32_t block_num) -> uint32_t;
  auto hash_dent_lock_id(ino_type par_ino, std::string_view file_name)
      -> uint32_t;
  auto hash_overflow_atable(ino_type ino, uint32_t block_num) -> uint32_t;
  auto get_timestamp() -> uint64_t;

private:
  auto truncate_next_file(std::string_view path)
      -> std::tuple<std::string_view, std::string_view, bool>;
  auto next_file_inode_offset() -> uint64_t;
  auto next_dir_inode_offset() -> uint64_t;
  template <typename T>
  auto read_inode(T *inode, int fd, uint64_t offset, uint64_t size = sizeof(T))
      -> ssize_t;
  auto read_inode(ino_type ino, ino_idx &inode_idx, void *buf,
                  bool file_meta_only = false) -> MetadataRespMsg;
  template <typename T>
  auto write_inode(T *inode, int fd, uint64_t offset, uint64_t size = sizeof(T))
      -> ssize_t;
  auto write_ext4(int fd, uint64_t offset, const void *data, size_t size)
      -> ssize_t;
  auto read_ext4(int fd, uint64_t offset, void *buf, size_t size) -> ssize_t;
  auto palign_up(uint64_t x, uint64_t a) -> uint64_t;
  auto read_block(ino_type ino, uint32_t block_num, void *buf, off_t offset,
                  size_t size) -> std::pair<MetadataRespMsg, ssize_t>;
  auto write_block(ino_type ino, uint32_t block_num, const void *buf,
                   off_t offset, size_t size)
      -> std::pair<MetadataRespMsg, ssize_t>;
  auto inline_write_block(ino_type ino, uint16_t offset, const void *buf,
                          size_t size) -> std::pair<MetadataRespMsg, ssize_t>;
  auto inline_block_migrate(ino_type ino, FileInode *file_inode)
      -> MetadataRespMsg;
  auto create_inode(ino_type ino, bool is_dir) -> MetadataRespMsg;
  auto add_dentry(const dentry_key &dent_key, dentry dent)
      -> std::pair<MetadataRespMsg, ino_type>;
  // remove dentry is different from delete dentry. remove dentry only remove
  // the dentry from dentry table, but delete dentry will remove the dentry and
  // inode and data block.
  auto remove_dentry(const dentry_key &dent_key)
      -> std::pair<MetadataRespMsg, dentry>;

  auto parse_one_level_path(std::string_view path, ino_type ino)
      -> std::tuple<MetadataRespMsg, std::string_view, ino_type>;

  auto update_dentry_cache(std::string_view remained_path, bool ignore_last_lvl,
                           size_t start_lvl, ino_type parent_ino,
                           ino_type medi_ino[]) -> void;

public:
  /*
    Local api, invoked directly by client, blocked
  */
  auto parse_path(const ParsePathArgs &args) -> ParsePathResult;
  auto bad_parse_path(const ParsePathArgs &args) -> ParsePathResult;
  void get_inode(const GetInodeArgs &args, GetInodeResult &ret);
  auto create(const CreateArgs &args) -> CreateResult;
  auto rename(const RenameArgs &args) -> RenameResult;
  auto write(const WriteArgs &args) -> WriteResult;
  auto write_vec(const WriteArgs *args, size_t size) -> WriteResult;
  auto inline_write(const InlineWriteArgs &args) -> InlineWriteResult;
  void inline_data_migrate(const InlineDataMigrateArgs &args,
                           InlineDataMigrateResult &ret);
  auto update_inode(const UpdateInodeArgs &args_tmp) -> UpdateInodeResult;
  auto read(const ReadArgs &args) -> ReadResult;
  auto delete_file(const DeleteArgs &args) -> DeleteResult;
  auto add_overflow_atable_entry(const AddOverflowATableEntryArgs &args)
      -> AddOverflowATableEntryResult;
  auto get_overflow_atable_entry(const GetOverflowATableEntryArgs &args)
      -> GetOverflowATableEntryResult;

  /*
  RPC handler
  */
  auto handle_parse_path_req(RequestParsePath *req) -> void;
  auto handle_get_inode_req(RequestGetInode *req) -> void;
  auto handle_add_dentry_req(RequestAddDentry *req) -> void;
  auto handle_create_inode_req(RequestCreateInode *req) -> void;
  auto handle_remove_dentry_req(RequestRemoveDentry *req) -> void;
  auto handle_write_req(RequestWrite *req, void *data) -> void;
  auto handle_inline_write_req(RequestInlineWrite *req, void *data) -> void;
  auto handle_inline_data_migrate_req(RequestInlineDataMigrate *req) -> void;
  auto handle_update_inode_req(RequestUpdateInode *req, void *data) -> void;
  auto handle_read_req(RequestRead *req) -> void;
  auto handle_delete_dentry_req(RequestDeleteDentry *req) -> ino_type;
  auto handle_delete_inode_req(RequestDeleteInode *req) -> void;
  auto handle_delete_data_req(RequestDeleteData *req) -> void;
  auto handle_add_overflow_atable_entry_req(RequestAddOverflowATableEntry *req)
      -> void;
  auto handle_get_overflow_atable_entry_req(RequestGetOverflowATableEntry *req)
      -> void;

  /*
   * KVDB
   */
  auto get_dentry_db() -> DentryDB * { return dentry_db_.get(); }
  auto get_meta_db() -> MetaDB * { return meta_db_.get(); }
  auto get_btable_db() -> BTableDB * { return btable_db_.get(); }
  auto get_overflow_atable_db() -> OverflowATableDB * {
    return overflow_atable_db_.get();
  }

private:
  std::unique_ptr<DentryDB> dentry_db_;
  std::unique_ptr<MetaDB> meta_db_;
  std::unique_ptr<BTableDB> btable_db_;
  std::unique_ptr<OverflowATableDB> overflow_atable_db_;
  std::unique_ptr<DataBlockManager> data_block_mgr_;
  std::unique_ptr<DentryCache> dentry_cache_;

private:
  std::atomic<uint64_t> next_file_inode_offset_{0};
  std::atomic<uint64_t> next_dir_inode_offset_{0};
  std::atomic<uint64_t> next_block_offset_{0};

  IdxFreeList file_inode_offset_free_list_{};
  IdxFreeList dir_inode_offset_free_list_{};

  int file_inode_fd_{};
  int dir_inode_fd_{};

  ino_type root_ino_; // use only if this node is root node

  Spinlock dent_locks_[dent_locks_cnt]{};
  std::mutex ino_locks_[ino_locks_cnt]{};
};

} // namespace nextfs
