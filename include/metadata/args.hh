#include "common/util.hh"
#include <cstdint>
#pragma once
#include "metadata/inode.hh"
#include "rpc/message.hh"
#include "string"
#include "vector"

namespace nextfs {

enum class ResultMsg : uint16_t {
  PARSE_PATH_SUCCESS = 1000,
  PARSE_PATH_ARGS_ERROR,
  PARSE_PATH_FAILED_BUT_FIND_PARENT,
  PARSE_PATH_FAILED_NOT_FIND_PARENT,

  GET_INODE_SUCCESS,
  GET_INODE_FAILED,

  CREATE_SUCCESS,
  CREATE_FAILED,
  FILE_ALREADY_EXIST,

  RENAME_SUCCESS,
  RENAME_FAILED,

  WRITE_SUCCESS,
  WRITE_FAILED,

  INLINE_WRITE_SUCCESS,
  INLINE_WRITE_FAILED,

  INLINE_DATA_MIGRATE_SUCCESS,
  INLINE_DATA_MIGRATE_FAILED,

  UPDATE_INODE_SUCCESS,
  UPDATE_INODE_FAILED,

  READ_SUCCESS,
  READ_FAILED,

  DELETE_SUCCESS,
  DELETE_FAILED,

  ADD_OVERFLOW_ATABLE_ENTRY_SUCCESS,
  ADD_OVERFLOW_ATABLE_ENTRY_FAILED,

  GET_OVERFLOW_ATABLE_ENTRY_SUCCESS,
  GET_OVERFLOW_ATABLE_ENTRY_FAILED,

};

struct ParsePathArgs {
  std::string total_path_;
};

// parse_path("/a/b/c/d/e/f")
// ->
// ret.medi_ino_[0] : '/' -> ino
// ret.medi_ino_[1] : 'a' -> ino
// ret.medi_ino_[2] : 'b' -> ino
// ret.medi_ino_[3] : 'c' -> ino
// ret.medi_ino_[4] : 'd' -> ino
struct ParsePathResult {
  ResultMsg msg_;
  ino_type target_ino_{};
  ino_type medi_ino_[ino_cache_level]{};
};

struct GetInodeArgs {
  ino_type file_ino_; // use parse_path() to find
};

struct GetInodeResult {
  ResultMsg msg_;
  bool is_dir_;
  union {
    DirInode dir_inode_;
    FileInode file_inode_;
  };
  GetInodeResult(){};
};

struct CreateArgs {
  bool is_dir_;
  ino_type parent_ino_; // use parse_path() to find
  ino_type ino_;        // use snowflake and consistent hash to generate
  std::string file_name_;
};

struct CreateResult {
  ResultMsg msg_;
  ino_type exist_ino_{};
};

struct RenameArgs {
  ino_type old_parent_ino_;
  ino_type new_parent_ino_;
  std::string old_name_;
  std::string new_name_;
};

struct RenameResult {
  ResultMsg msg_;
};

struct WriteArgs {
  ino_type file_ino_;
  uint32_t data_node_id_; // the node to which the data is actually written
  uint32_t block_num_;    // write to which block
  uint32_t offset_;       // offset in block
  uint32_t data_size_;    // data size
  void *data_;            //
};

struct WriteResult {
  ResultMsg msg_;
  uint32_t write_size_;
  // uint32_t target_block_size_;
};

struct InlineWriteArgs {
  ino_type file_ino_;
  uint16_t offset_;
  uint16_t data_size_;
  char *data_;
};

struct InlineWriteResult {
  ResultMsg msg_;
  uint32_t write_size_;
  uint32_t inline_data_size_;
};

struct InlineDataMigrateArgs {
  ino_type file_ino_;
};

struct InlineDataMigrateResult {
  ResultMsg msg_;
  FileInode file_inode_;
};

struct UpdateInodeArgs {
  ino_type file_ino_;
  uint64_t file_size_;                                // new file size
  std::vector<std::pair<uint32_t, uint32_t>> blocks_; // <block_idx, node_id>
  // uint32_t block_num_; // start
  // uint32_t block_cnt_; // count
  // std::vector<uint32_t> node_ids_;
};

struct UpdateInodeResult {
  ResultMsg msg_;
};

// Read a complete block of file
struct ReadArgs {
  ino_type file_ino_;
  uint32_t node_id_; // the block data's position, we can get this value through
                     // get_inode()
  uint32_t block_num_;
  uint32_t offset_; // offset in block
  uint32_t read_size_;

  // void *data_;
};

struct ReadResult {
  ResultMsg msg_;
  uint32_t read_size_;
  std::pair<uint32_t, void *> data_blk_{}; // MAX default
};

struct DeleteArgs {
  ino_type parent_ino_;
  std::string file_name_; // last part
};

struct DeleteResult {
  ResultMsg msg_;
};

struct AddOverflowATableEntryArgs {
  ino_type file_ino_;
  uint32_t block_num_;
  uint32_t node_id_;
};

struct AddOverflowATableEntryResult {
  ResultMsg msg_;
};

struct GetOverflowATableEntryArgs {
  ino_type file_ino_;
  uint32_t block_num_;
};

struct GetOverflowATableEntryResult {
  uint32_t node_id_;
  ResultMsg msg_;
};

} // namespace nextfs