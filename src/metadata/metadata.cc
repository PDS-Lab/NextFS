#include "metadata/metadata.hh"
#include "algorithm"
#include "ankerl/unordered_dense.h"
#include "common/config.hh"
#include "common/futex.hh"
#include "common/logger.hh"
#include "common/spinlock.hh"
#include "common/util.hh"
#include "daemon/env.hh"
#include "iostream"
#include "metadata/aio.hh"
#include "metadata/args.hh"
#include "metadata/data_block_mgr.hh"
#include "metadata/inode.hh"
#include "metadata/util/coding.hh"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "rpc/handler.hh"
#include "rpc/message.hh"
#include "unistd.h"
#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <memory>
#include <mutex>
#include <string_view>
#include <sys/types.h>
#include <tuple>
#include <utility>
#include <vector>

namespace nextfs {

MetadataHandler::MetadataHandler(string dentry_db_path,
                                 Options dentry_db_options, string meta_db_path,
                                 Options meta_db_options, string btable_db_path,
                                 Options btable_db_options,
                                 string overflow_atable_db_path,
                                 Options overflow_atable_db_options) {

  // multi Rocksdb use shared thread pool
  rocksdb::Env *env = rocksdb::Env::Default();
  env->SetBackgroundThreads(5, rocksdb::Env::LOW);
  env->SetBackgroundThreads(5, rocksdb::Env::HIGH);
  dentry_db_options.env = env;
  dentry_db_options.max_background_jobs = 10;

  meta_db_options.env = env;
  meta_db_options.max_background_jobs = 10;

  btable_db_options.env = env;
  btable_db_options.max_background_jobs = 10;

  overflow_atable_db_options.env = env;
  overflow_atable_db_options.max_background_compactions = 10;

  dentry_db_ =
      std::make_unique<DentryDB>(dentry_db_options, std::move(dentry_db_path));
  meta_db_ = std::make_unique<MetaDB>(meta_db_options, std::move(meta_db_path));
  btable_db_ =
      std::make_unique<BTableDB>(btable_db_options, std::move(btable_db_path));
  overflow_atable_db_ = std::make_unique<OverflowATableDB>(
      overflow_atable_db_options, std::move(overflow_atable_db_path));

  uint32_t node_id = GlobalEnv::node_id();
  Snowflake::init(node_id);

  root_ino_.ino_high_ = 0;
  root_ino_.ino_low_ = GlobalEnv::config().root_node_id_;
  root_ino_.group_id_ = get_group_id(GlobalEnv::config().root_node_id_);
  root_ino_.server_id_ = get_server_id(GlobalEnv::config().root_node_id_);

  // open ext4 file

  file_inode_fd_ = open(GlobalEnv::config().file_inode_path_.c_str(),
                        O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  if (file_inode_fd_ == -1) {
    warn("ext4 file for file_inode open failed");
  }

  dir_inode_fd_ = open(GlobalEnv::config().dir_inode_path_.c_str(),
                       O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  if (dir_inode_fd_ == -1) {
    warn("ext4 file for dir_inode open failed");
  }

  data_block_mgr_ = std::make_unique<DataBlockManager>(
      GlobalEnv::config().data_block_dir_, GlobalEnv::config().block_size_);
  if (GlobalEnv::config().dentry_cache_valid_time_ms_ > 0) {
    dentry_cache_ = std::make_unique<DentryCache>(
        GlobalEnv::config().dentry_cache_valid_time_ms_ * 1000 * 1000);
  } else {
    dentry_cache_ = nullptr;
  }
}

MetadataHandler::~MetadataHandler() = default;

/*
Some Utils
*/

// use this func if data can not be contained in msg
auto MetadataHandler::rpc_send_blk(uint32_t node_id, BlockInfo *blk,
                                   size_t length) -> void {
  length += 4;
  if (length > msg_head_size) {
    warn("send msg too long");
    return;
  }

  info("send blk to node {}", node_id);
  GlobalEnv::rpc()->send_rpc_req(node_id, blk, length);
}

// only used for msg with data.
// the data_addr must be in the MR !
// If data and head are in the same MR, set data_lkey to -1
auto MetadataHandler::rpc_send_with_data(uint32_t node_id, BlockInfo *head_blk,
                                         std::pair<uint32_t, void *> data_blk,
                                         size_t data_size) -> void {
  info("send data msg to node {}", node_id);
  GlobalEnv::rpc()->send_rpc_req_with_data(node_id, head_blk, data_blk,
                                           data_size);
} // namespace nextfs

auto MetadataHandler::hash_server_id(ino_type ino) -> uint32_t {
  string key = ino_type_serialize(ino);
  return ConsistentHash::get_server_id(key);
}

auto MetadataHandler::hash_dent_lock_id(ino_type par_ino,
                                        std::string_view file_name)
    -> uint32_t {
  using namespace ankerl::unordered_dense::detail;
  uint64_t b = wyhash::hash(file_name.data(), file_name.size());
  return wyhash::mix(par_ino.hash(), b) % dent_locks_cnt;
}

auto MetadataHandler::hash_write_node(ino_type ino, uint32_t block_num)
    -> uint32_t {
  string key = std::to_string(block_num) + ino_type_serialize(ino);
  return ConsistentHash::get_server_id(key);
}

auto MetadataHandler::hash_overflow_atable(ino_type ino, uint32_t block_num)
    -> uint32_t {
  string key = overflow_atable_key_serialize({ino, block_num});
  return ConsistentHash::get_server_id(key);
}

auto MetadataHandler::truncate_next_file(std::string_view path)
    -> std::tuple<std::string_view, std::string_view, bool> {
  bool is_last = false;
  std::string_view next_file{};
  if (path == "") {
    next_file = "";
  } else if (path[0] == '/') {
    next_file = "/";
    path = path.substr(1);
  } else {
    size_t pos = path.find('/');
    next_file = path.substr(0, pos);
    if (pos == string::npos)
      path = "";
    else
      path = path.substr(pos + 1);
  }
  is_last = path == "";
  return {path, next_file, is_last};
}

auto MetadataHandler::next_file_inode_offset() -> uint64_t {
  // check weather there is free idx
  uint64_t offset = file_inode_offset_free_list_.get_one_free_idx();
  if (offset == -1) {
    offset = next_file_inode_offset_.fetch_add(FILE_INODE_SIZE,
                                               std::memory_order_relaxed);
  }
  return offset;
}

auto MetadataHandler::next_dir_inode_offset() -> uint64_t {
  // check weather there is free idx
  uint64_t offset = dir_inode_offset_free_list_.get_one_free_idx();
  if (offset == -1) {
    offset = next_dir_inode_offset_.fetch_add(DIR_INODE_SIZE,
                                              std::memory_order_relaxed);
  }
  return offset;
}

auto MetadataHandler::get_timestamp() -> uint64_t {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
};

auto MetadataHandler::palign_up(uint64_t x, uint64_t a) -> uint64_t {
  return ((x + (a - 1)) & ~(a - 1));
};

template <typename T>
auto MetadataHandler::read_inode(T *inode, int fd, uint64_t offset,
                                 uint64_t size) -> ssize_t {
  return read_ext4(fd, offset, static_cast<void *>(inode), size);
}

auto MetadataHandler::read_inode(ino_type ino, ino_idx &inode_idx, void *buf,
                                 bool file_meta_only) -> MetadataRespMsg {
  Status s = meta_db_->get_meta(ino, &inode_idx);
  if (!s.ok()) {
    return MetadataRespMsg::INODE_NOT_FOUND;
  }
  if (inode_idx.type_ == Type::Directory) {
    ssize_t n = read_inode(static_cast<DirInode *>(buf), dir_inode_fd_,
                           inode_idx.offset_);
    return n == DIR_INODE_SIZE ? MetadataRespMsg::INODE_FOUND
                               : MetadataRespMsg::INODE_NOT_FOUND;
  } else {
    ssize_t n = read_inode(static_cast<FileInode *>(buf), file_inode_fd_,
                           inode_idx.offset_,
                           file_meta_only ? inode_meta_size : FILE_INODE_SIZE);
    return n == (file_meta_only ? inode_meta_size : FILE_INODE_SIZE)
               ? MetadataRespMsg::INODE_FOUND
               : MetadataRespMsg::INODE_NOT_FOUND;
  }
}

template <typename T>
auto MetadataHandler::write_inode(T *inode, int fd, uint64_t offset,
                                  uint64_t size) -> ssize_t {
  return write_ext4(fd, offset, static_cast<const void *>(inode), size);
}

auto MetadataHandler::write_ext4(int fd, uint64_t offset, const void *data,
                                 size_t size) -> ssize_t {
  ssize_t n = 0;
  while (n < size) {
    ssize_t write_size = pwrite(fd, static_cast<const uint8_t *>(data) + n,
                                size - n, offset + n);
    if (write_size == -1) {
      return -1;
    }
    if (write_size == 0) {
      return n;
    }
    n += write_size;
  }
  return n;
}

auto MetadataHandler::read_ext4(int fd, uint64_t offset, void *buf, size_t size)
    -> ssize_t {
  ssize_t n = 0;
  while (n < size) {
    ssize_t read_size =
        pread(fd, static_cast<uint8_t *>(buf) + n, size - n, offset + n);
    if (read_size == -1) {
      return -1;
    }
    if (read_size == 0) {
      return n;
    }
    n += read_size;
  }
  return n;
}

auto MetadataHandler::read_block(ino_type ino, uint32_t block_num, void *buf,
                                 off_t offset, size_t size)
    -> std::pair<MetadataRespMsg, ssize_t> {
  if (offset + size > GlobalEnv::config().block_size_) {
    return {MetadataRespMsg::READ_FAILED, -1};
  }
  btable_key block_key = {ino, block_num};

  btable_item block_info;
  // get block info in Btable
  Status s = btable_db_->get_block_info(block_key, &block_info);
  if (!s.ok()) {
    // error("get block info failed. ino: {}, block_num: {}", ino, block_num);
    warn("get btable item failed, block_num: {}", block_num);
    return {MetadataRespMsg::BTABLE_NOT_FIND_THE_BLOCK, -1};
  }
  ssize_t read_size = read_ext4(data_block_mgr_->get_fd(block_info.file_id_),
                                block_info.offset_ + offset, buf, size);
  return {read_size >= 0 ? MetadataRespMsg::READ_SUCCESS
                         : MetadataRespMsg::READ_FAILED,
          read_size};
}

auto MetadataHandler::write_block(ino_type ino, uint32_t block_num,
                                  const void *buf, off_t offset, size_t size)
    -> std::pair<MetadataRespMsg, ssize_t> {
  if (offset + size > GlobalEnv::config().block_size_) {
    return {MetadataRespMsg::WRITE_DATA_TOO_LONG, -1};
  }
  btable_key block_key = {ino, block_num};

  btable_item block_info;
  // get block info in Btable
  Status s = btable_db_->get_block_info(block_key, &block_info);
  if (!s.ok()) {
    // block not in the b table, so generate one
    block_info = data_block_mgr_->alloc_block();
    s = btable_db_->put_block_info(block_key, block_info);
    if (!s.ok()) {
      data_block_mgr_->free_block(block_info);
      return {MetadataRespMsg::WRITE_BTABLE_FAILED, -1};
    }
  }
  ssize_t write_size = write_ext4(data_block_mgr_->get_fd(block_info.file_id_),
                                  block_info.offset_ + offset, buf, size);
  if (write_size < 0) {
    data_block_mgr_->free_block(block_info);
    return {MetadataRespMsg::WRITE_DATA_FAILED, -1};
  }
  return {MetadataRespMsg::WRITE_SUCCESS, write_size};
}

auto MetadataHandler::inline_write_block(ino_type ino, uint16_t offset,
                                         const void *buf, size_t size)
    -> std::pair<MetadataRespMsg, ssize_t> {
  if (offset + size > inline_data_size_limit) {
    return {MetadataRespMsg::INLINE_WRITE_DATA_TOO_LONG, -1};
  }
  ino_idx inode_idx;
  FileInode file_inode;
  auto r = read_inode(ino, inode_idx, &file_inode);
  if (r != MetadataRespMsg::INODE_FOUND || inode_idx.type_ != Type::File) {
    return {MetadataRespMsg::INLINE_WRITE_DATA_FAILED, -1};
  }
  // read_size >= 0
  memcpy(file_inode.inline_data_ + offset, buf, size);
  file_inode.file_size_ = std::max(file_inode.file_size_, offset + size);
  file_inode.access_time_ = get_timestamp();
  file_inode.modify_time_ = file_inode.access_time_;
  ssize_t write_size =
      write_inode(&file_inode, file_inode_fd_, inode_idx.offset_);
  return {write_size == FILE_INODE_SIZE
              ? MetadataRespMsg::INLINE_WRITE_SUCCESS
              : MetadataRespMsg::INLINE_WRITE_DATA_FAILED,
          file_inode.file_size_};
}

auto MetadataHandler::inline_block_migrate(ino_type ino, FileInode *file_inode)
    -> MetadataRespMsg {
  std::lock_guard l(ino_locks_[ino.hash() % ino_locks_cnt]);
  ino_idx inode_idx;
  auto r = read_inode(ino, inode_idx, file_inode);
  if (r == MetadataRespMsg::INODE_NOT_FOUND) {
    return MetadataRespMsg::INLINE_DATA_MIGRATE_INODE_NOT_FOUND;
  }
  if (inode_idx.type_ != Type::File) {
    return MetadataRespMsg::INLINE_DATA_MIGRATE_INODE_IS_DIR;
  }
  if (file_inode->has_atable_) {
    return MetadataRespMsg::INLINE_DATA_MIGRATE_ATABLE_ALREADY_EXISTS;
  }
  // inline data size
  uint64_t file_size = file_inode->file_size_;
  btable_item block_info = data_block_mgr_->alloc_block();
  size_t write_size =
      write_ext4(data_block_mgr_->get_fd(block_info.file_id_),
                 block_info.offset_, file_inode->inline_data_, file_size);
  if (write_size != file_size) {
    data_block_mgr_->free_block(block_info);
    return MetadataRespMsg::INLINE_DATA_MIGRATE_FAILED;
  }

  btable_key block_key = {ino, 0};
  Status s = btable_db_->put_block_info(block_key, block_info);
  if (!s.ok()) {
    data_block_mgr_->free_block(block_info);
    return MetadataRespMsg::INLINE_DATA_MIGRATE_FAILED;
  }

  memset(file_inode->inline_data_, '\0', inline_data_size_limit);
  file_inode->has_atable_ = true;
  file_inode->cast_ATable()->map_[0] = GlobalEnv::node_id();

  write_size = write_inode(file_inode, file_inode_fd_, inode_idx.offset_);
  if (write_size != FILE_INODE_SIZE) {
    data_block_mgr_->free_block(block_info);
    return MetadataRespMsg::INLINE_DATA_MIGRATE_FAILED;
  }
  return MetadataRespMsg::INLINE_DATA_MIGRATE_SUCCESS;
}

auto MetadataHandler::create_inode(ino_type ino, bool is_dir)
    -> MetadataRespMsg {
  info("create inode, file ino hash is : {}", ino.hash());
  ino_idx inode_idx;
  inode_idx.type_ = is_dir ? Type::Directory : Type::File;
  inode_idx.offset_ =
      is_dir ? next_dir_inode_offset() : next_file_inode_offset();
  Status s = meta_db_->put_meta(ino, inode_idx);
  if (!s.ok()) {
    warn("create_inode() put_meta failed");
    if (is_dir) {
      dir_inode_offset_free_list_.put_one_free_idx(inode_idx.offset_);
    } else {
      file_inode_offset_free_list_.put_one_free_idx(inode_idx.offset_);
    }
    return MetadataRespMsg::CREATE_INODE_FAILED;
  }
  if (is_dir) {
    DirInode dir_inode;
    dir_inode.perm_ = 7;
    dir_inode.create_time_ = get_timestamp();
    dir_inode.access_time_ = dir_inode.create_time_;
    dir_inode.modify_time_ = dir_inode.create_time_;
    ssize_t write_size =
        write_inode(&dir_inode, dir_inode_fd_, inode_idx.offset_);
    if (write_size != DIR_INODE_SIZE) {
      dir_inode_offset_free_list_.put_one_free_idx(inode_idx.offset_);
      return MetadataRespMsg::CREATE_INODE_FAILED;
    }
  } else {
    FileInode file_inode;
    file_inode.perm_ = 7;
    file_inode.create_time_ = get_timestamp();
    file_inode.access_time_ = file_inode.create_time_;
    file_inode.modify_time_ = file_inode.create_time_;
    file_inode.file_size_ = 0;
    file_inode.has_atable_ = false;
    ssize_t write_size =
        write_inode(&file_inode, file_inode_fd_, inode_idx.offset_);
    if (write_size != FILE_INODE_SIZE) {
      file_inode_offset_free_list_.put_one_free_idx(inode_idx.offset_);
      return MetadataRespMsg::CREATE_INODE_FAILED;
    }
  }
  return MetadataRespMsg::CREATE_INODE_SUCCESS;
}

auto MetadataHandler::add_dentry(const dentry_key &dent_key, dentry dent)
    -> std::pair<MetadataRespMsg, ino_type> {
  std::lock_guard<Spinlock> lock(
      dent_locks_[hash_dent_lock_id(dent_key.ino_, dent_key.subdir_name_)]);

  std::pair<MetadataRespMsg, ino_type> ret;

  dentry dent_tmp{};
  Status s = dentry_db_->get_dentry(dent_key, &dent_tmp);
  if (s.ok()) {
    ret.first = MetadataRespMsg::DENTRY_ALREADY_EXIST;
    ret.second = dent_tmp.sub_ino_;
    return ret;
  } else {
    s = dentry_db_->put_dentry(dent_key, dent);
    ret.first = s.ok() ? MetadataRespMsg::ADD_DENTRY_SUCCESS
                       : MetadataRespMsg::ADD_DENTRY_FAILED;
    return ret;
  }
}

auto MetadataHandler::remove_dentry(const dentry_key &dent_key)
    -> std::pair<MetadataRespMsg, dentry> {
  std::lock_guard<Spinlock> lock(
      dent_locks_[hash_dent_lock_id(dent_key.ino_, dent_key.subdir_name_)]);
  dentry dent_tmp{};
  Status s = dentry_db_->get_dentry(dent_key, &dent_tmp);
  if (!s.ok()) {
    return {MetadataRespMsg::DENTRY_NOT_EXIST, dent_tmp};
  } else {
    s = dentry_db_->delete_dentry(dent_key);
    return {s.ok() ? MetadataRespMsg::REMOVE_DENTRY_SUCCESS
                   : MetadataRespMsg::REMOVE_DENTRY_FAILED,
            dent_tmp};
  }
}

auto MetadataHandler::parse_one_level_path(std::string_view path, ino_type ino)
    -> std::tuple<MetadataRespMsg, std::string_view, ino_type> {
  std::string_view remained_path, sub_file_name;
  bool is_last;
  std::tie(remained_path, sub_file_name, is_last) = truncate_next_file(path);
  if (sub_file_name == "/" &&
      GlobalEnv::node_id() != GlobalEnv::config().root_node_id_) {
    return {MetadataRespMsg::PARSE_PATH_FAILED_RPC_WRONG_NODE, remained_path,
            ino_type{}};
  }
  if (sub_file_name == "/" && is_last) {
    return {MetadataRespMsg::PARSE_PATH_SUCCESS, remained_path, root_ino_};
  }
  if (sub_file_name == "/" && !is_last) {
    ino = root_ino_;
    std::tie(remained_path, sub_file_name, is_last) =
        truncate_next_file(remained_path);
  }
  info("sub_file_name : {}", sub_file_name);
  info("remained_path : {}", remained_path);
  info("is_last : {}", is_last);
  dentry_key dent_key;
  dentry dent;
  dent_key.ino_ = ino;
  strncpy(dent_key.subdir_name_, sub_file_name.data(), sub_file_name.size());
  dent_key.subdir_name_[sub_file_name.size()] = '\0';
  Status s = dentry_db_->get_dentry(dent_key, &dent);
  if (!s.ok()) {
    return {is_last ? MetadataRespMsg::PARSE_PATH_FAILED_BUT_FIND_PARENT
                    : MetadataRespMsg::PARSE_PATH_FAILED_NOT_FIND_PARENT,
            remained_path, ino_type{}};
  }
  return {MetadataRespMsg::PARSE_PATH_SUCCESS, remained_path, dent.sub_ino_};
}

auto MetadataHandler::update_dentry_cache(std::string_view remained_path,
                                          bool ignore_last_lvl,
                                          size_t start_lvl, ino_type parent_ino,
                                          ino_type medi_ino[]) -> void {
  if (start_lvl >= ino_cache_level) {
    return;
  }
  if (dentry_cache_ == nullptr) {
    return;
  }
  std::string_view truncated_path, sub_file_name;
  bool is_last;
  while (start_lvl < ino_cache_level && !remained_path.empty()) {
    std::tie(truncated_path, sub_file_name, is_last) =
        truncate_next_file(remained_path);
    if (is_last && ignore_last_lvl) {
      break;
    }

    dentry_key dent_key;
    dent_key.ino_ = parent_ino;
    strncpy(dent_key.subdir_name_, sub_file_name.data(), sub_file_name.size());
    dent_key.subdir_name_[sub_file_name.size()] = '\0';
    dentry_cache_->put_dentry(dent_key, medi_ino[start_lvl]);
    parent_ino = medi_ino[start_lvl];
    start_lvl++;
    remained_path = truncated_path;
  }
}

/*
Local API
*/

auto MetadataHandler::parse_path(const ParsePathArgs &args) -> ParsePathResult {
  ParsePathResult ret;
  if (args.total_path_[0] != '/') {
    ret.msg_ = ResultMsg::PARSE_PATH_ARGS_ERROR;
    return ret;
  }
  std::string_view remained_path = args.total_path_;
  remained_path = remained_path.substr(1); // remove the first '/'
  ino_type sub_ino = root_ino_;
  uint32_t cur_level = 0;

  // try using cache first
  while (cur_level < ino_cache_level && !remained_path.empty() &&
         dentry_cache_ != nullptr) {
    std::string_view truncated_path, sub_file_name;
    bool is_last;
    std::tie(truncated_path, sub_file_name, is_last) =
        truncate_next_file(remained_path);

    dentry_key dent_key;
    ino_type ino;
    dent_key.ino_ = sub_ino;
    strncpy(dent_key.subdir_name_, sub_file_name.data(), sub_file_name.size());
    dent_key.subdir_name_[sub_file_name.size()] = '\0';
    if (!dentry_cache_->get_dentry(dent_key, &ino)) {
      break;
    }
    ret.medi_ino_[cur_level] = ino;
    cur_level++;
    sub_ino = ino;
    remained_path = truncated_path;
  }
  if (remained_path.empty()) {
    ret.msg_ = ResultMsg::PARSE_PATH_SUCCESS;
    ret.target_ino_ = sub_ino;
    return ret;
  }
  ino_type uncached_parent_ino = sub_ino;
  uint32_t uncached_level = cur_level;
  std::string_view uncached_path = remained_path;

  MetadataRespMsg r;
  // if (cur_level < ino_cache_level) {
  //   ret.medi_ino_[cur_level] = root_ino_;
  //   cur_level++;
  // }
  while (sub_ino.node_id() == GlobalEnv::node_id()) {
    std::tie(r, remained_path, sub_ino) =
        parse_one_level_path(remained_path, sub_ino);
    bool is_last = remained_path == "";

    if (cur_level < ino_cache_level) {
      ret.medi_ino_[cur_level] = sub_ino;
      cur_level++;
    }

    if (r == MetadataRespMsg::PARSE_PATH_FAILED_RPC_WRONG_NODE) {
      ret.msg_ = ResultMsg::PARSE_PATH_FAILED_NOT_FIND_PARENT;
      ret.target_ino_ = sub_ino;
      return ret;
    }
    if (r == MetadataRespMsg::PARSE_PATH_FAILED_BUT_FIND_PARENT) {
      ret.msg_ = ResultMsg::PARSE_PATH_FAILED_BUT_FIND_PARENT;
      ret.target_ino_ = sub_ino;
      return ret;
    }
    if (r == MetadataRespMsg::PARSE_PATH_FAILED_NOT_FIND_PARENT) {
      ret.msg_ = ResultMsg::PARSE_PATH_FAILED_NOT_FIND_PARENT;
      ret.target_ino_ = sub_ino;
      return ret;
    }
    if (is_last && r == MetadataRespMsg::PARSE_PATH_SUCCESS) {
      ret.msg_ = ResultMsg::PARSE_PATH_SUCCESS;
      ret.target_ino_ = sub_ino;
      return ret;
    }
  }

  // send RequestParsePath to root node;
  BlockInfo *blk = GlobalEnv::rpc()->get_rpc_req_block();
  auto *req = reinterpret_cast<RequestParsePath *>(blk->addr_);
  req->type_ = RpcMessageType::RPC_TYPE_METADATA_PARSE_PATH_REQ;
  strncpy(req->remained_path_, remained_path.data(), remained_path.size());
  req->remained_path_[remained_path.size()] = '\0';
  req->cur_ino_ = sub_ino;
  req->client_id_ = GlobalEnv::instance().node_id();
  memcpy(req->medi_ino_, ret.medi_ino_, sizeof(ret.medi_ino_));
  req->cur_level_ = cur_level;

  FutexParker parker(true);
  ResponseParsePath resp;
  bool completed = false;

  req->notify_ = {
      reinterpret_cast<uint64_t>(&completed),
      reinterpret_cast<uint64_t>(&parker),
      reinterpret_cast<uint64_t>(&resp),
  };

  size_t size = sizeof(RequestParsePath);
  rpc_send_blk(sub_ino.node_id(), blk, size);

  while (!completed) {
    parker.park(); // wait the resp
  }

  // wake up
  if (resp.msg_ == MetadataRespMsg::PARSE_PATH_SUCCESS) {
    ret.msg_ = ResultMsg::PARSE_PATH_SUCCESS;
    ret.target_ino_ = resp.target_ino_;
  } else {
    if (resp.msg_ == MetadataRespMsg::PARSE_PATH_FAILED_NOT_FIND_PARENT)
      ret.msg_ = ResultMsg::PARSE_PATH_FAILED_NOT_FIND_PARENT;
    else if (resp.msg_ == MetadataRespMsg::PARSE_PATH_FAILED_BUT_FIND_PARENT)
      ret.msg_ = ResultMsg::PARSE_PATH_FAILED_BUT_FIND_PARENT;
  }
  memcpy(ret.medi_ino_, resp.medi_ino_, sizeof(resp.medi_ino_));
  // warn("parse path success, ino: {} {} {} {} {}", ret.medi_ino_[0].hash(),
  //      ret.medi_ino_[1].hash(), ret.medi_ino_[2].hash(),
  //      ret.medi_ino_[3].hash(), ret.medi_ino_[4].hash());
  // update cache
  if (ret.msg_ == ResultMsg::PARSE_PATH_SUCCESS ||
      ret.msg_ == ResultMsg::PARSE_PATH_FAILED_BUT_FIND_PARENT) {
    update_dentry_cache(
        uncached_path, ret.msg_ == ResultMsg::PARSE_PATH_FAILED_BUT_FIND_PARENT,
        uncached_level, uncached_parent_ino, ret.medi_ino_);
  }
  return ret;
}

auto MetadataHandler::bad_parse_path(const ParsePathArgs &args)
    -> ParsePathResult {
  ParsePathResult ret;
  ret.msg_ = ResultMsg::PARSE_PATH_SUCCESS;

  std::string_view remained_path = args.total_path_;
  std::string_view sub_file_name;
  bool is_last = false;
  ino_type cur_ino;
  cur_ino.server_id_ = get_server_id(GlobalEnv::config().root_node_id_);
  cur_ino.group_id_ = get_group_id(GlobalEnv::config().root_node_id_);
  while (!is_last) {
    std::tie(remained_path, sub_file_name, is_last) =
        truncate_next_file(remained_path);
    if (cur_ino.node_id() == GlobalEnv::node_id()) {
      if (sub_file_name == "/") {
        cur_ino = root_ino_;
        continue;
      }
      dentry_key dent_key;
      dentry dent;
      dent_key.ino_ = cur_ino;
      strncpy(dent_key.subdir_name_, sub_file_name.data(),
              sub_file_name.size());
      dent_key.subdir_name_[sub_file_name.size()] = '\0';
      Status s = dentry_db_->get_dentry(dent_key, &dent);
      if (!s.ok()) {
        ret.msg_ = is_last ? ResultMsg::PARSE_PATH_FAILED_BUT_FIND_PARENT
                           : ResultMsg::PARSE_PATH_FAILED_NOT_FIND_PARENT;
        ret.target_ino_ = cur_ino;
        return ret;
      }
      cur_ino = dent.sub_ino_;
      ret.target_ino_ = cur_ino;
      continue;
    }

    // send RequestParsePath
    BlockInfo *blk = GlobalEnv::rpc()->get_rpc_req_block();
    auto *req = reinterpret_cast<RequestParsePath *>(blk->addr_);
    req->type_ = RpcMessageType::RPC_TYPE_METADATA_PARSE_PATH_REQ;
    strncpy(req->remained_path_, sub_file_name.data(), sub_file_name.size());
    req->remained_path_[sub_file_name.size()] = '\0';
    req->cur_ino_ = cur_ino;
    req->client_id_ = GlobalEnv::instance().node_id();

    FutexParker parker(true);
    ResponseParsePath resp;
    bool completed = false;

    req->notify_ = {
        reinterpret_cast<uint64_t>(&completed),
        reinterpret_cast<uint64_t>(&parker),
        reinterpret_cast<uint64_t>(&resp),
    };
    size_t size = sizeof(RequestParsePath);
    rpc_send_blk(cur_ino.node_id(), blk, size);

    while (!completed) {
      parker.park(); // wait the resp
    }

    if (resp.msg_ == MetadataRespMsg::PARSE_PATH_SUCCESS) {
      ret.msg_ = ResultMsg::PARSE_PATH_SUCCESS;
      cur_ino = resp.target_ino_;
      ret.target_ino_ = cur_ino;
    } else {
      ret.msg_ = is_last ? ResultMsg::PARSE_PATH_FAILED_BUT_FIND_PARENT
                         : ResultMsg::PARSE_PATH_FAILED_NOT_FIND_PARENT;
      ret.target_ino_ = cur_ino;
      return ret;
    }
  }
  return ret;
}

void MetadataHandler::get_inode(const GetInodeArgs &args, GetInodeResult &ret) {
  if (args.file_ino_.node_id() == GlobalEnv::node_id()) {
    ino_idx inode_idx;
    auto r = read_inode(args.file_ino_, inode_idx, &ret.file_inode_);
    ret.is_dir_ = inode_idx.type_ == Type::Directory;
    ret.msg_ = r == MetadataRespMsg::INODE_FOUND ? ResultMsg::GET_INODE_SUCCESS
                                                 : ResultMsg::GET_INODE_FAILED;
    return;
  }

  // send RequestGetInode to target node
  BlockInfo *blk = GlobalEnv::rpc()->get_rpc_req_block();
  auto *req = reinterpret_cast<RequestGetInode *>(blk->addr_);
  req->type_ = RpcMessageType::RPC_TYPE_GET_INODE_REQ;
  req->file_ino_ = args.file_ino_;
  req->client_id_ = GlobalEnv::instance().node_id();

  FutexParker parker(true);
  ResponseGetInode resp;
  bool completed = false;

  req->notify_.complete_addr_ = reinterpret_cast<uint64_t>(&completed);
  req->notify_.futex_addr_ = reinterpret_cast<uint64_t>(&parker);
  req->notify_.resp_addr_ = reinterpret_cast<uint64_t>(&resp);
  req->notify_.data_addr_ = reinterpret_cast<uint64_t>(&ret.file_inode_);

  size_t size = sizeof(RequestGetInode);
  rpc_send_blk(req->file_ino_.node_id(), blk, size);
  while (!completed) {
    parker.park(); // wait the resp
  }

  // wake up
  if (resp.msg_ == MetadataRespMsg::INODE_FOUND) {
    ret.msg_ = ResultMsg::GET_INODE_SUCCESS;
    ret.is_dir_ = resp.is_dir_;
  } else {
    ret.msg_ = ResultMsg::GET_INODE_FAILED;
  }
}

// assume we create "c" in "/a/b"
// the parent_ino_ is "b"'s ino, the ino_ is "c"'s ino
// func create()'s steps are as follow:
// 2. send to "c"'s node to add meta-c and "c"'s inode
// 1. send to "b"'s node to add dentry-c

auto MetadataHandler::create(const CreateArgs &args) -> CreateResult {
  CreateResult ret;
  ino_type parent_ino = args.parent_ino_;
  ino_type child_ino = args.ino_;
  FutexParker parker(true);
  bool completed = false;

  // create inode
  ResponseCreateInode resp;
  completed = false;

  if (child_ino.node_id() == GlobalEnv::node_id()) {
    // create inode locally
    auto r = create_inode(child_ino, args.is_dir_);
    if (r != MetadataRespMsg::CREATE_INODE_SUCCESS) {
      ret.msg_ = ResultMsg::CREATE_FAILED;
      trace("create_inode() failed");
      return ret;
    }
  } else {
    // send to "c"'s node to add meta-c and inode
    BlockInfo *blk = GlobalEnv::rpc()->get_rpc_req_block();
    auto *create_inode_req = reinterpret_cast<RequestCreateInode *>(blk->addr_);
    create_inode_req->type_ = RpcMessageType::RPC_TYPE_CREATE_INODE_REQ;
    create_inode_req->client_id_ = GlobalEnv::instance().node_id();
    create_inode_req->ino_ = child_ino;
    create_inode_req->is_dir_ = args.is_dir_;

    create_inode_req->notify_ = {reinterpret_cast<uint64_t>(&completed),
                                 reinterpret_cast<uint64_t>(&parker),
                                 reinterpret_cast<uint64_t>(&resp)};

    rpc_send_blk(child_ino.node_id(), blk, sizeof(RequestCreateInode));
    while (!completed) {
      parker.park(); // wait the resp
    }

    // wake up
    ret = {};
    if (resp.msg_ != MetadataRespMsg::CREATE_INODE_SUCCESS) {
      trace("resp.msg_ != MetadataRespMsg::CREATE_INODE_SUCCESS");
      ret.msg_ = ResultMsg::CREATE_FAILED;
      return ret;
    }
    // success, go on
  }

  // create dentry
  completed = false;
  dentry_key dent_key;
  dent_key.ino_ = parent_ino;
  strcpy(dent_key.subdir_name_, args.file_name_.c_str());
  dentry dent{child_ino.group_id(), child_ino.server_id(), child_ino};

  if (parent_ino.node_id() == GlobalEnv::node_id()) {
    auto [r, file_ino] = add_dentry(dent_key, dent);
    if (r == MetadataRespMsg::ADD_DENTRY_FAILED) {
      ret.msg_ = ResultMsg::CREATE_FAILED;
      trace("add_dentry() failed");
      return ret;
    } else if (r == MetadataRespMsg::DENTRY_ALREADY_EXIST) {
      ret.msg_ = ResultMsg::FILE_ALREADY_EXIST;
      ret.exist_ino_ = file_ino;
      return ret;
    }
    ret.msg_ = ResultMsg::CREATE_SUCCESS;
    // success, go on
  } else {
    // send to "b"'s node to add dentry-c
    BlockInfo *blk_add_dentry = GlobalEnv::rpc()->get_rpc_req_block();
    auto *add_dentry_req =
        reinterpret_cast<RequestAddDentry *>(blk_add_dentry->addr_);
    add_dentry_req->type_ = RpcMessageType::RPC_TYPE_ADD_DENTRY_REQ;
    add_dentry_req->client_id_ = GlobalEnv::instance().node_id();
    add_dentry_req->dent_key_ = dent_key;
    add_dentry_req->dent_ = dent;

    ResponseAddDentry resp2;
    add_dentry_req->notify_ = {reinterpret_cast<uint64_t>(&completed),
                               reinterpret_cast<uint64_t>(&parker),
                               reinterpret_cast<uint64_t>(&resp2)};

    rpc_send_blk(parent_ino.node_id(), blk_add_dentry,
                 sizeof(RequestAddDentry));
    while (!completed) {
      parker.park(); // wait the resp 1
    }

    // wake up
    if (resp2.msg_ != MetadataRespMsg::ADD_DENTRY_SUCCESS) {
      if (resp2.msg_ == MetadataRespMsg::DENTRY_ALREADY_EXIST) {
        ret.msg_ = ResultMsg::FILE_ALREADY_EXIST;
        ret.exist_ino_ = resp2.exist_ino_;
        // ps:
        // Do not roll back create_inode, which wastes some resources, but can
        // ensure consistency.
      } else {
        trace("resp2.msg_ : {}", (uint32_t)resp2.msg_);
        ret.msg_ = ResultMsg::CREATE_FAILED;
      }
    } else {
      ret.msg_ = ResultMsg::CREATE_SUCCESS;
    }
  }
  return ret;
}

// 1.
// how can we rename "/a/b/c" to "/a/b/x"
// remove dentry-c  (b's node, old_parent)
// add dentry-x  (b's node, new parent)
// 2.
// how can we rename "/a/b/c" to "/a/x"
// remove dentry-c (b's node, old-parent)
// add dentry-x (a's node, new-parent)
auto MetadataHandler::rename(const RenameArgs &args) -> RenameResult {
  ino_type old_parent_ino = args.old_parent_ino_;
  ino_type new_parent_ino = args.new_parent_ino_;
  FutexParker parker(true);
  bool completed = false;

  // send RequestRemoveDentry to old_parent_ino node
  dentry_key old_dent_key;
  old_dent_key.ino_ = old_parent_ino;
  strcpy(old_dent_key.subdir_name_, args.old_name_.c_str());
  dentry old_dent;

  if (old_parent_ino.node_id() == GlobalEnv::node_id()) {
    auto [r, dent] = remove_dentry(old_dent_key);
    if (r != MetadataRespMsg::REMOVE_DENTRY_SUCCESS) {
      RenameResult ret;
      ret.msg_ = ResultMsg::RENAME_FAILED;
      return ret;
    }
    old_dent = dent;
  } else {
    BlockInfo *blk = GlobalEnv::rpc()->get_rpc_req_block();
    auto *rm_dentry_req = reinterpret_cast<RequestRemoveDentry *>(blk->addr_);
    rm_dentry_req->type_ = RpcMessageType::RPC_TYPE_REMOVE_DENTRY_REQ;
    rm_dentry_req->client_id_ = GlobalEnv::instance().node_id();
    rm_dentry_req->dent_key_ = old_dent_key;
    ResponseRemoveDentry resp;
    rm_dentry_req->notify_ = {reinterpret_cast<uint64_t>(&completed),
                              reinterpret_cast<uint64_t>(&parker),
                              reinterpret_cast<uint64_t>(&resp)};

    size_t size = sizeof(RequestRemoveDentry);
    rpc_send_blk(old_parent_ino.node_id(), blk, size);
    while (!completed) {
      parker.park(); // wait the resp
    }

    // wake up
    if (resp.msg_ != MetadataRespMsg::REMOVE_DENTRY_SUCCESS) {
      RenameResult ret;
      ret.msg_ = ResultMsg::RENAME_FAILED;
      return ret;
    }
    old_dent = resp.dent_;
  }

  // send RequestAddDentry to new_parent_ino node
  dentry_key new_dent_key;
  new_dent_key.ino_ = new_parent_ino;
  strcpy(new_dent_key.subdir_name_, args.new_name_.c_str());

  if (new_parent_ino.node_id() == GlobalEnv::node_id()) {
    auto [r, file_inode] = add_dentry(new_dent_key, old_dent);
    if (r != MetadataRespMsg::ADD_DENTRY_SUCCESS) {
      RenameResult ret;
      ret.msg_ = ResultMsg::RENAME_FAILED;
      return ret;
    }
    RenameResult ret;
    ret.msg_ = ResultMsg::RENAME_SUCCESS;
    return ret;
  }

  BlockInfo *blk_add_dent = GlobalEnv::rpc()->get_rpc_req_block();
  auto *add_dentry_req =
      reinterpret_cast<RequestAddDentry *>(blk_add_dent->addr_);
  add_dentry_req->type_ = RpcMessageType::RPC_TYPE_ADD_DENTRY_REQ;
  add_dentry_req->client_id_ = GlobalEnv::instance().node_id();
  add_dentry_req->dent_key_ = new_dent_key;
  add_dentry_req->dent_ = old_dent;
  ResponseRemoveDentry resp2;
  completed = false;
  add_dentry_req->notify_ = {reinterpret_cast<uint64_t>(&completed),
                             reinterpret_cast<uint64_t>(&parker),
                             reinterpret_cast<uint64_t>(&resp2)};

  rpc_send_blk(new_parent_ino.node_id(), blk_add_dent,
               sizeof(RequestAddDentry));
  while (!completed) {
    parker.park(); // wait the resp
  }

  // wake up
  RenameResult ret;
  if (resp2.msg_ != MetadataRespMsg::ADD_DENTRY_SUCCESS) {
    ret.msg_ = ResultMsg::RENAME_FAILED;
    return ret;
  }

  // success
  ret.msg_ = ResultMsg::RENAME_SUCCESS;
  return ret;
}

// only call write() when the file has A table.
// write() is only responsible for writing data and updating the B table, not
// the A table (inode)
// the step that write() should do is only one : send RequestWrite to
// args.node_id
auto MetadataHandler::write(const WriteArgs &args) -> WriteResult {
  WriteResult ret;
  if (args.data_node_id_ == GlobalEnv::node_id()) {
    auto [r, n] = write_block(args.file_ino_, args.block_num_, args.data_,
                              args.offset_, args.data_size_);
    ret.write_size_ = n;
    ret.msg_ = r == MetadataRespMsg::WRITE_SUCCESS && n == args.data_size_
                   ? ResultMsg::WRITE_SUCCESS
                   : ResultMsg::WRITE_FAILED;
    return ret;
  }
  BlockInfo *head_blk = GlobalEnv::rpc()->get_rpc_req_block();
  auto *req = reinterpret_cast<RequestWrite *>(head_blk->addr_);
  req->type_ = RpcMessageType::RPC_TYPE_WRITE_REQ;
  req->client_id_ = GlobalEnv::node_id();
  req->file_ino_ = args.file_ino_;
  req->block_num_ = args.block_num_;
  req->data_size_ = args.data_size_;
  req->offset_ = args.offset_;

  FutexParker parker(true);
  ResponseWrite resp;
  bool completed = false;
  req->notify_ = {reinterpret_cast<uint64_t>(&completed),
                  reinterpret_cast<uint64_t>(&parker),
                  reinterpret_cast<uint64_t>(&resp)};

  // RpcMessage msg;
  // msg.type_ = RpcMessageType::RPC_TYPE_WRITE_REQ;
  // msg.data_.write_req_ = req;
  // size_t msg_size = sizeof(msg.type_) + sizeof(msg.data_.write_req_) + 8;
  //
  std::pair<uint32_t, void *> data_blk = std::make_pair<uint32_t, void *>(
      std::numeric_limits<uint32_t>::max(), (void *)args.data_);
  rpc_send_with_data(args.data_node_id_, head_blk, data_blk, args.data_size_);

  while (!completed) {
    parker.park();
  }

  // wake up
  if (resp.msg_ != MetadataRespMsg::WRITE_SUCCESS) {
    ret.msg_ = ResultMsg::WRITE_FAILED;
    return ret;
  }

  // success
  ret.msg_ = ResultMsg::WRITE_SUCCESS;
  ret.write_size_ = resp.write_size_;
  // ret.target_block_size_ = resp.target_block_size_;
  return ret;
}

auto MetadataHandler::write_vec(const WriteArgs *args_vec, size_t size)
    -> WriteResult {
  struct RemoteReq {
    BlockInfo *blk_;
    RequestWrite *req_;
    ResponseWrite resp_;
    const WriteArgs *args_;
    bool completed = false;
  };
  struct LocalReq {
    const WriteArgs *args_;
    btable_item block_info_;
  };
  std::vector<RemoteReq> remote_reqs;
  std::vector<LocalReq> local_reqs;
  std::vector<btable_key> local_req_keys;
  for (size_t i = 0; i < size; i++) {
    if (args_vec[i].offset_ + args_vec[i].data_size_ >
        GlobalEnv::config().block_size_) {
      return {ResultMsg::WRITE_FAILED, 0};
    }
    if (args_vec[i].data_node_id_ == GlobalEnv::node_id()) {
      local_req_keys.push_back({args_vec[i].file_ino_, args_vec[i].block_num_});
      local_reqs.push_back({&args_vec[i], {}});
    } else {
      BlockInfo *head_blk = GlobalEnv::rpc()->get_rpc_req_block();
      auto *req = reinterpret_cast<RequestWrite *>(head_blk->addr_);
      req->type_ = RpcMessageType::RPC_TYPE_WRITE_REQ;
      req->client_id_ = GlobalEnv::node_id();
      req->file_ino_ = args_vec[i].file_ino_;
      req->block_num_ = args_vec[i].block_num_;
      req->data_size_ = args_vec[i].data_size_;
      req->offset_ = args_vec[i].offset_;

      RemoteReq remote_req;
      remote_req.blk_ = head_blk;
      remote_req.req_ = req;
      remote_req.args_ = &args_vec[i];
      remote_reqs.push_back(remote_req);
    }
  }
  //! buffer of local_reqs and remote_reqs are stable now, we can use their addr
  FutexParker parker{true};
  for (auto &req : remote_reqs) {
    req.req_->notify_ = {reinterpret_cast<uint64_t>(&req.completed),
                         reinterpret_cast<uint64_t>(&parker),
                         reinterpret_cast<uint64_t>(&req.resp_)};
    auto shm_data_l_key = GlobalEnv::instance().rpc()->data_mr()->lkey;
    rpc_send_with_data(req.args_->data_node_id_, req.blk_,
                       {std::numeric_limits<uint32_t>::max(), req.args_->data_},
                       req.args_->data_size_);
  }

  AioEngine::IoReq local_aio_reqs[local_reqs.size()];
  {
    btable_item blk_infos[local_req_keys.size()];
    auto s_list = btable_db_->get_block_infos(local_req_keys.data(), blk_infos,
                                              local_req_keys.size());
    btable_key add_btable[local_req_keys.size()];
    btable_item add_btable_item[local_req_keys.size()];
    size_t to_add = 0;
    for (size_t i = 0; i < local_req_keys.size(); i++) {
      if (!s_list[i].ok()) {
        add_btable[to_add] = local_req_keys[i];
        add_btable_item[to_add] = data_block_mgr_->alloc_block();
        local_reqs[i].block_info_ = add_btable_item[to_add];
        to_add++;
      } else {
        local_reqs[i].block_info_ = blk_infos[i];
      }
    }
    auto s = btable_db_->put_block_infos(add_btable, add_btable_item, to_add);
    if (!s.ok()) {
      return {ResultMsg::WRITE_FAILED, 0};
    }
    for (size_t i = 0; i < local_reqs.size(); i++) {
      local_aio_reqs[i] = AioEngine::IoReq::write(
          data_block_mgr_->get_fd(local_reqs[i].block_info_.file_id_),
          local_reqs[i].args_->data_, local_reqs[i].args_->data_size_,
          local_reqs[i].args_->offset_ + local_reqs[i].block_info_.offset_);
    }
    AioEngine::submit(local_aio_reqs, local_reqs.size());
  }

  // wait resp
  for (auto &req : remote_reqs) {
    while (!req.completed) {
      parker.park();
    }
  }
  for (auto &req : local_aio_reqs) {
    while (!req.done()) {
      AioEngine::wait_ioevent(local_reqs.size());
    }
  }
  for (auto &req : remote_reqs) {
    if (req.resp_.msg_ != MetadataRespMsg::WRITE_SUCCESS) {
      return {ResultMsg::WRITE_FAILED, 0};
    }
  }
  for (auto &req : local_aio_reqs) {
    if (req.res() != req.req_size()) {
      return {ResultMsg::WRITE_FAILED, 0};
    }
  }
  return {ResultMsg::WRITE_SUCCESS, 0};
}

// write the data to inode.
// only call inline_write() when the file does not have A table.
auto MetadataHandler::inline_write(const InlineWriteArgs &args)
    -> InlineWriteResult {
  InlineWriteResult ret;
  if (args.file_ino_.node_id() == GlobalEnv::node_id()) {
    auto [r, file_size] = inline_write_block(args.file_ino_, args.offset_,
                                             args.data_, args.data_size_);
    ret.write_size_ = args.data_size_;
    ret.inline_data_size_ = file_size;
    ret.msg_ = r == MetadataRespMsg::INLINE_WRITE_SUCCESS
                   ? ResultMsg::INLINE_WRITE_SUCCESS
                   : ResultMsg::INLINE_WRITE_FAILED;
    return ret;
  }
  BlockInfo *head_blk = GlobalEnv::rpc()->get_rpc_req_block();
  auto *req = reinterpret_cast<RequestInlineWrite *>(head_blk->addr_);
  req->type_ = RpcMessageType::RPC_TYPE_INLINE_WRITE_REQ;
  req->client_id_ = GlobalEnv::node_id();
  req->file_ino_ = args.file_ino_;
  req->data_size_ = args.data_size_;
  req->offset_ = args.offset_;

  // info("memcpy start");
  // info("args.data: {}", args.data_);
  // info("args.size: {}", args.size_);

  FutexParker parker(true);
  ResponseInlineWrite resp;
  bool completed = false;
  req->notify_ = {reinterpret_cast<uint64_t>(&completed),
                  reinterpret_cast<uint64_t>(&parker),
                  reinterpret_cast<uint64_t>(&resp)};
  size_t size = sizeof(RequestInlineWrite) + args.data_size_;

  std::pair<uint32_t, void *> data_blk = std::make_pair<uint32_t, void *>(
      std::numeric_limits<uint32_t>::max(), args.data_);
  rpc_send_with_data(args.file_ino_.node_id(), head_blk, data_blk,
                     args.data_size_);

  while (!completed) {
    parker.park();
  }

  // wake up
  if (resp.msg_ != MetadataRespMsg::INLINE_WRITE_SUCCESS) {
    ret.msg_ = ResultMsg::INLINE_WRITE_FAILED;
    return ret;
  }

  // success
  ret.msg_ = ResultMsg::INLINE_WRITE_SUCCESS;
  ret.write_size_ = resp.write_size_;
  ret.inline_data_size_ = resp.inline_data_size_;
  return ret;
}

// migrate the inline data to data_storage_file (ext4),
// and set 'has_atable_' to 'true'.
void MetadataHandler::inline_data_migrate(const InlineDataMigrateArgs &args,
                                          InlineDataMigrateResult &ret) {
  if (args.file_ino_.node_id() == GlobalEnv::node_id()) {
    auto r = inline_block_migrate(args.file_ino_, &ret.file_inode_);
    ret.msg_ = (r == MetadataRespMsg::INLINE_DATA_MIGRATE_SUCCESS ||
                r == MetadataRespMsg::INLINE_DATA_MIGRATE_ATABLE_ALREADY_EXISTS)
                   ? ResultMsg::INLINE_DATA_MIGRATE_SUCCESS
                   : ResultMsg::INLINE_DATA_MIGRATE_FAILED;
    return;
  }
  BlockInfo *blk = GlobalEnv::rpc()->get_rpc_req_block();
  auto *req = reinterpret_cast<RequestInlineDataMigrate *>(blk->addr_);
  req->type_ = RpcMessageType::RPC_TYPE_INLINE_DATA_MIGRATE_REQ;
  req->client_id_ = GlobalEnv::node_id();
  req->file_ino_ = args.file_ino_;

  FutexParker parker(true);
  ResponseInlineDataMigrate resp;
  bool completed = false;
  req->notify_.complete_addr_ = reinterpret_cast<uint64_t>(&completed);
  req->notify_.futex_addr_ = reinterpret_cast<uint64_t>(&parker);
  req->notify_.resp_addr_ = reinterpret_cast<uint64_t>(&resp);
  req->notify_.data_addr_ = reinterpret_cast<uint64_t>(&ret.file_inode_);
  size_t size = sizeof(RequestInlineDataMigrate);
  rpc_send_blk(args.file_ino_.node_id(), blk, size);

  while (!completed) {
    parker.park();
  }

  // wake up
  if (resp.msg_ == MetadataRespMsg::INLINE_DATA_MIGRATE_SUCCESS ||
      resp.msg_ == MetadataRespMsg::INLINE_DATA_MIGRATE_ATABLE_ALREADY_EXISTS) {
    ret.msg_ = ResultMsg::INLINE_DATA_MIGRATE_SUCCESS;
  } else {
    ret.msg_ = ResultMsg::INLINE_DATA_MIGRATE_FAILED;
  }
}

// 4 things to be updated
// a_time, m_time, file_size, A_table
auto MetadataHandler::update_inode(const UpdateInodeArgs &args_tmp)
    -> UpdateInodeResult {
  UpdateInodeResult ret;
  UpdateInodeArgs args = args_tmp;

  size_t shard = args.file_ino_.hash() % ino_locks_cnt;
  struct QNode {
    bool done{false};
    UpdateInodeResult ret;
    const UpdateInodeArgs *arg;
    std::condition_variable cv;
  };
  static ankerl::unordered_dense::map<ino_type, std::deque<QNode *>>
      batch_map[ino_locks_cnt];
  QNode qnode;
  size_t batch_num;
  qnode.arg = &args;
  {
    std::unique_lock<std::mutex> lock(ino_locks_[shard]);
    auto it = batch_map[shard].find(args.file_ino_);
    if (it == batch_map[shard].end()) {
      it =
          batch_map[shard].emplace(args.file_ino_, std::deque<QNode *>()).first;
    }
    it->second.push_back(&qnode);
    while (!qnode.done && &qnode != batch_map[shard][args.file_ino_].front()) {
      qnode.cv.wait(lock);
    }
    if (qnode.done) {
      return qnode.ret;
    }
    // we are the leader now
    it = batch_map[shard].find(args.file_ino_);
    batch_num = it->second.size();
    for (size_t i = 1; i < batch_num; i++) {
      args.file_size_ =
          std::max(args.file_size_, it->second[i]->arg->file_size_);
      args.blocks_.insert(args.blocks_.end(),
                          it->second[i]->arg->blocks_.begin(),
                          it->second[i]->arg->blocks_.end());
    }
  }

  auto defer = [&](ResultMsg msg) {
    std::unique_lock<std::mutex> lock(ino_locks_[shard]);
    auto it = batch_map[shard].find(args.file_ino_);
    for (size_t i = 0; i < batch_num; i++) {
      it->second.front()->done = true;
      it->second.front()->ret.msg_ = msg;
      it->second.front()->cv.notify_one();
      it->second.pop_front();
    }
    if (it->second.empty()) {
      batch_map[shard].erase(it);
    } else {
      it->second.front()->cv.notify_one(); // wake new leader
    }
  };

  if (args.file_ino_.node_id() == GlobalEnv::node_id()) {
    ino_idx inode_idx;
    FileInode file_inode;
    {
      uint64_t ts = get_timestamp();
      std::lock_guard<std::mutex> lock(ino_locks_[shard]);
      auto r = read_inode(args.file_ino_, inode_idx, &file_inode,
                          args.blocks_.empty());
      if (r != MetadataRespMsg::INODE_FOUND) {
        ret.msg_ = ResultMsg::UPDATE_INODE_FAILED;
        defer(ret.msg_);
        return ret;
      }
      file_inode.access_time_ = ts;
      file_inode.modify_time_ = ts;
      file_inode.file_size_ = std::max(file_inode.file_size_, args.file_size_);
      for (auto &b : args.blocks_) {
        file_inode.cast_ATable()->map_[b.first] = b.second;
      }
      ssize_t write_size =
          write_inode(&file_inode, file_inode_fd_, inode_idx.offset_,
                      args.blocks_.empty() ? inode_meta_size : FILE_INODE_SIZE);
      ret.msg_ = write_size == (args.blocks_.empty() ? inode_meta_size
                                                     : FILE_INODE_SIZE)
                     ? ResultMsg::UPDATE_INODE_SUCCESS
                     : ResultMsg::UPDATE_INODE_FAILED;
    }
    defer(ret.msg_);
    return ret;
  }

  BlockInfo *blk = GlobalEnv::rpc()->get_rpc_req_block();
  auto *req = (RequestUpdateInode *)(blk->addr_);
  req->type_ = RpcMessageType::RPC_TYPE_UPDATE_INODE_REQ;
  req->client_id_ = GlobalEnv::node_id();
  req->file_ino_ = args.file_ino_;
  req->file_size_ = args.file_size_;
  req->block_cnt_ = args.blocks_.size();

  FutexParker parker(true);
  ResponseUpdateInode resp;
  bool completed = false;
  req->notify_ = {reinterpret_cast<uint64_t>(&completed),
                  reinterpret_cast<uint64_t>(&parker),
                  reinterpret_cast<uint64_t>(&resp)};

  if (req->block_cnt_ > 0) {
    auto data_blk = GlobalEnv::block_pool()->alloc<void>().value();
    void *data = reinterpret_cast<char *>(data_blk.second);

    for (size_t i = 0; i < args.blocks_.size(); i++) {
      *((uint32_t *)data + 2 * i) = args.blocks_[i].first;
      *((uint32_t *)data + 2 * i + 1) = args.blocks_[i].second;
    }
    size_t data_size = req->block_cnt_ * 2 * sizeof(uint32_t);
    rpc_send_with_data(args.file_ino_.node_id(), blk, data_blk, data_size);
  } else {
    size_t size = sizeof(RequestUpdateInode);
    rpc_send_blk(args.file_ino_.node_id(), blk, size);
  }

  while (!completed) {
    parker.park();
  }

  // wake up
  if (resp.msg_ != MetadataRespMsg::UPDATE_INODE_SUCCESS) {
    ret.msg_ = ResultMsg::UPDATE_INODE_FAILED;
  } else {
    ret.msg_ = ResultMsg::UPDATE_INODE_SUCCESS;
  }
  defer(ret.msg_);
  return ret;
}

auto MetadataHandler::read(const ReadArgs &args) -> ReadResult {
  ReadResult ret;
  if (args.node_id_ == GlobalEnv::node_id()) {
    auto data_blk = GlobalEnv::block_pool()->alloc<void>().value();
    auto [r, n] = read_block(args.file_ino_, args.block_num_, data_blk.second,
                             args.offset_, args.read_size_);
    ret.msg_ = r == MetadataRespMsg::READ_SUCCESS ? ResultMsg::READ_SUCCESS
                                                  : ResultMsg::READ_FAILED;

    ret.read_size_ = n;
    ret.data_blk_ = data_blk;
    return ret;
  }

  BlockInfo *blk = GlobalEnv::rpc()->get_rpc_req_block();
  auto *req = reinterpret_cast<RequestRead *>(blk->addr_);
  req->type_ = RpcMessageType::RPC_TYPE_READ_REQ;
  req->client_id_ = GlobalEnv::node_id();
  req->file_ino_ = args.file_ino_;
  req->block_num_ = args.block_num_;
  req->offset_ = args.offset_;
  req->read_size_ = args.read_size_;

  FutexParker parker(true);
  MetadataRespMsg msg;
  bool completed = false;

  req->notify_.complete_addr_ = reinterpret_cast<uint64_t>(&completed);
  req->notify_.futex_addr_ = reinterpret_cast<uint64_t>(&parker);
  req->notify_.msg_addr_ = reinterpret_cast<uint64_t>(&msg);
  // req->notify_.data_addr_ = reinterpret_cast<uint64_t>(args.data_);
  std::pair<uint32_t, void *> data_blk{};
  req->notify_.data_addr_ = reinterpret_cast<uint64_t>(&data_blk);

  size_t size = sizeof(RequestRead);
  rpc_send_blk(args.node_id_, blk, size);
  while (!completed) {
    parker.park();
  }
  ret.data_blk_ = data_blk;

  // wake up
  if (msg != MetadataRespMsg::READ_SUCCESS) {
    ret.msg_ = ResultMsg::READ_FAILED;
    return ret;
  }

  // success
  ret.msg_ = ResultMsg::READ_SUCCESS;
  return ret;
}

auto MetadataHandler::delete_file(const DeleteArgs &args) -> DeleteResult {
  // send RequestDeleteDentry to parent's node.
  // that node will delete dentry and then send RequestDeleteInode and
  // RequestDeleteData to other nodes.
  BlockInfo *blk = GlobalEnv::rpc()->get_rpc_req_block();

  ino_type ino;
  DeleteResult ret;

  // delete dentry
  auto *req_del_dent = reinterpret_cast<RequestDeleteDentry *>(blk->addr_);
  req_del_dent->type_ = RpcMessageType::RPC_TYPE_DELETE_DENTRY_REQ;
  req_del_dent->client_id_ = GlobalEnv::node_id();
  req_del_dent->parent_ino_ = args.parent_ino_;
  strcpy(req_del_dent->file_name_, args.file_name_.c_str());
  if (args.parent_ino_.node_id() == GlobalEnv::node_id()) {
    ino = handle_delete_dentry_req(req_del_dent);
    GlobalEnv::rpc()->memory_pool()->put_one_block(blk);
  } else {
    FutexParker parker(true);
    ResponseDeleteDentry resp_del_dent;
    bool completed = false;
    req_del_dent->notify_ = {reinterpret_cast<uint64_t>(&completed),
                             reinterpret_cast<uint64_t>(&parker),
                             reinterpret_cast<uint64_t>(&resp_del_dent)};
    size_t size = sizeof(RequestDeleteDentry);
    rpc_send_blk(args.parent_ino_.node_id(), blk, size);

    while (!completed) {
      parker.park();
    }

    if (resp_del_dent.msg_ != MetadataRespMsg::DELETE_DENTRY_SUCCESS) {
      ret.msg_ = ResultMsg::DELETE_FAILED;
      return ret;
    }

    ino = resp_del_dent.file_ino_;
  }

  if (ino.node_id() == 0) { // invalid ino
    ret.msg_ = ResultMsg::DELETE_FAILED;
    return ret;
  }
  if (dentry_cache_ != nullptr) {
    dentry_key dent_key;
    dent_key.ino_ = args.parent_ino_;
    strcpy(dent_key.subdir_name_, args.file_name_.c_str());
    dentry_cache_->delete_dentry(dent_key);
  }

  // delete inode
  BlockInfo *blk2 = GlobalEnv::rpc()->get_rpc_req_block();
  auto *req_del_inode = reinterpret_cast<RequestDeleteInode *>(blk2->addr_);
  req_del_inode->type_ = RpcMessageType::RPC_TYPE_DELETE_INODE_REQ;
  req_del_inode->file_ino_ = ino;
  req_del_inode->client_id_ = GlobalEnv::node_id();
  size_t size = sizeof(RequestDeleteInode);
  if (ino.node_id() == GlobalEnv::node_id()) {
    handle_delete_inode_req(req_del_inode);
    GlobalEnv::rpc()->memory_pool()->put_one_block(blk2);
    ret.msg_ = ResultMsg::DELETE_SUCCESS;
    return ret;
  }

  FutexParker parker(true);
  ResponseDeleteInode resp_del_inode;
  bool completed = false;
  req_del_inode->notify_ = {reinterpret_cast<uint64_t>(&completed),
                            reinterpret_cast<uint64_t>(&parker),
                            reinterpret_cast<uint64_t>(&resp_del_inode)};
  size = sizeof(RequestDeleteInode);
  rpc_send_blk(ino.node_id(), blk2, size);

  while (!completed) {
    parker.park();
  }

  if (resp_del_inode.msg_ != MetadataRespMsg::DELETE_INODE_SUCCESS) {
    ret.msg_ = ResultMsg::DELETE_FAILED;
  } else {
    ret.msg_ = ResultMsg::DELETE_SUCCESS;
  }
  return ret;
}

// 1. This api does not affect the inode itself. If the file size is changed,
// you should call update_inode() to update the file size.
// 2. Send directly to the target node to add a new entry. The target node
// depends on the consistency hash.
auto MetadataHandler::add_overflow_atable_entry(
    const AddOverflowATableEntryArgs &args) -> AddOverflowATableEntryResult {
  AddOverflowATableEntryResult ret;
  uint32_t target_node = hash_overflow_atable(args.file_ino_, args.block_num_);
  if (target_node == GlobalEnv::node_id()) {
    overflow_atable_key atable_key = {args.file_ino_, args.block_num_};
    overflow_atable_item atable_item = {args.node_id_};
    Status s = overflow_atable_db_->put_entry(atable_key, atable_item);
    ret.msg_ = s.ok() ? ResultMsg::ADD_OVERFLOW_ATABLE_ENTRY_SUCCESS
                      : ResultMsg::ADD_OVERFLOW_ATABLE_ENTRY_FAILED;
    return ret;
  }

  BlockInfo *blk = GlobalEnv::rpc()->get_rpc_req_block();
  auto *req = reinterpret_cast<RequestAddOverflowATableEntry *>(blk->addr_);
  req->type_ = RpcMessageType::RPC_TYPE_ADD_OVERFLOW_ATABLE_ENTRY_REQ;
  req->client_id_ = GlobalEnv::node_id();
  req->file_ino_ = args.file_ino_;
  req->block_num_ = args.block_num_;
  req->node_id_ = args.node_id_;

  FutexParker parker(true);
  ResponseAddOverflowATableEntry resp;
  bool completed = false;
  req->notify_ = {reinterpret_cast<uint64_t>(&completed),
                  reinterpret_cast<uint64_t>(&parker),
                  reinterpret_cast<uint64_t>(&resp)};

  size_t size = sizeof(RequestAddOverflowATableEntry);
  rpc_send_blk(target_node, blk, size);
  while (!completed) {
    parker.park();
  }

  // wake up
  if (resp.msg_ != MetadataRespMsg::ADD_OVERFLOW_ATABLE_ENTRY_SUCCESS) {
    ret.msg_ = ResultMsg::ADD_OVERFLOW_ATABLE_ENTRY_FAILED;
  } else {
    ret.msg_ = ResultMsg::ADD_OVERFLOW_ATABLE_ENTRY_SUCCESS;
  }

  return ret;
};

auto MetadataHandler::get_overflow_atable_entry(
    const GetOverflowATableEntryArgs &args) -> GetOverflowATableEntryResult {
  uint32_t target_node = hash_overflow_atable(args.file_ino_, args.block_num_);
  GetOverflowATableEntryResult ret;
  if (target_node == GlobalEnv::node_id()) {
    overflow_atable_key atable_key = {args.file_ino_, args.block_num_};
    overflow_atable_item atable_item;
    Status s = overflow_atable_db_->get_entry(atable_key, &atable_item);
    ret.node_id_ = atable_item.node_id_;
    ret.msg_ = s.ok() ? ResultMsg::GET_OVERFLOW_ATABLE_ENTRY_SUCCESS
                      : ResultMsg::GET_OVERFLOW_ATABLE_ENTRY_FAILED;
    return ret;
  }

  BlockInfo *blk = GlobalEnv::rpc()->get_rpc_req_block();
  auto *req = reinterpret_cast<RequestGetOverflowATableEntry *>(blk->addr_);
  req->type_ = RpcMessageType::RPC_TYPE_GET_OVERFLOW_ATABLE_ENTRY_REQ;
  req->client_id_ = GlobalEnv::node_id();
  req->block_num_ = args.block_num_;
  req->file_ino_ = args.file_ino_;
  size_t size = sizeof(RequestGetOverflowATableEntry);

  FutexParker parker(true);
  ResponseGetOverflowATableEntry resp;
  bool completed = false;
  req->notify_ = {reinterpret_cast<uint64_t>(&completed),
                  reinterpret_cast<uint64_t>(&parker),
                  reinterpret_cast<uint64_t>(&resp)};

  rpc_send_blk(target_node, blk, size);
  while (!completed) {
    parker.park();
  }

  // wake up
  ret.node_id_ = resp.node_id_;
  if (resp.msg_ != MetadataRespMsg::GET_OVERFLOW_ATABLE_ENTRY_SUCCESS) {
    ret.msg_ = ResultMsg::GET_OVERFLOW_ATABLE_ENTRY_FAILED;
  } else {
    ret.msg_ = ResultMsg::GET_OVERFLOW_ATABLE_ENTRY_SUCCESS;
  }
  return ret;
};

// ============================= //

/*
  handle RPC request
*/
auto MetadataHandler::handle_parse_path_req(RequestParsePath *req) -> void {

  uint32_t client_id = req->client_id_;

  // string sub_file_name;
  // bool is_last = truncate_next_file(
  //     remained_path, sub_file_name); // it will truncate the remained_path

  BlockInfo *blk = GlobalEnv::rpc()->get_rpc_req_block();
  auto *resp = reinterpret_cast<ResponseParsePath *>(blk->addr_);
  resp->type_ = RpcMessageType::RPC_TYPE_METADATA_PARSE_PATH_RESP;
  resp->notify_ = req->notify_;

  MetadataRespMsg r;
  std::string_view remained_path = req->remained_path_;
  ino_type sub_ino = req->cur_ino_;
  bool is_last;

  // if (remained_path[0] == '/' &&
  //     GlobalEnv::node_id() == GlobalEnv::config().root_node_id_) {
  //   assert(req->cur_level_ == 0);
  //   req->medi_ino_[req->cur_level_] = root_ino_;
  //   req->cur_level_++;
  // }

  while (sub_ino.node_id() == GlobalEnv::node_id() ||
         (remained_path[0] == '/' &&
          GlobalEnv::node_id() == GlobalEnv::config().root_node_id_)) {
    std::tie(r, remained_path, sub_ino) =
        parse_one_level_path(remained_path, sub_ino);
    if (req->cur_level_ < ino_cache_level) {
      req->medi_ino_[req->cur_level_] = sub_ino;
      req->cur_level_++;
    }

    is_last = remained_path == "";
    if (r != MetadataRespMsg::PARSE_PATH_SUCCESS) {
      resp->msg_ = r;
      resp->target_ino_ = sub_ino;
      memcpy(resp->medi_ino_, req->medi_ino_, sizeof(req->medi_ino_));
      if (client_id == GlobalEnv::node_id()) {
        rpc::handle_parse_path_resp(resp);
        GlobalEnv::rpc()->memory_pool()->put_one_block(blk);
      } else {
        rpc_send_blk(client_id, blk, sizeof(ResponseParsePath));
      }
      return;
    }
    if (is_last) {
      resp->msg_ = MetadataRespMsg::PARSE_PATH_SUCCESS;
      resp->target_ino_ = sub_ino;
      memcpy(resp->medi_ino_, req->medi_ino_, sizeof(req->medi_ino_));
      if (client_id == GlobalEnv::node_id()) {
        rpc::handle_parse_path_resp(resp);
        GlobalEnv::rpc()->memory_pool()->put_one_block(blk);
      } else {
        rpc_send_blk(client_id, blk, sizeof(ResponseParsePath));
      }
      return;
    }
  }
  // if the next node is not the current node, send rpc to it
  auto *next_req = reinterpret_cast<RequestParsePath *>(blk->addr_);
  next_req->type_ = RpcMessageType::RPC_TYPE_METADATA_PARSE_PATH_REQ;
  next_req->notify_ = req->notify_;
  next_req->client_id_ = req->client_id_;
  memcpy(next_req->medi_ino_, req->medi_ino_, sizeof(req->medi_ino_));
  next_req->cur_level_ = req->cur_level_;
  strncpy(next_req->remained_path_, remained_path.data(), remained_path.size());
  next_req->remained_path_[remained_path.size()] = '\0';
  next_req->cur_ino_ = sub_ino;
  rpc_send_blk(next_req->cur_ino_.node_id(), blk, sizeof(RequestParsePath));
  return;
}

auto MetadataHandler::handle_get_inode_req(RequestGetInode *req) -> void {
  BlockInfo *blk = GlobalEnv::rpc()->get_rpc_req_block();

  uint32_t client_id = req->client_id_;
  ino_type file_ino = req->file_ino_;

  // get inode index
  auto *resp = (ResponseGetInode *)(blk->addr_);
  resp->type_ = RpcMessageType::RPC_TYPE_GET_INODE_RESP;
  resp->notify_ = req->notify_;

  ino_idx inode_idx;
  auto data_blk = GlobalEnv::block_pool()->alloc<void>().value();
  void *buf = data_blk.second;

  auto r = read_inode(file_ino, inode_idx, buf);
  if (r != MetadataRespMsg::INODE_FOUND) {
    // send error resp
    resp->type_ = RpcMessageType::RPC_TYPE_GET_INODE_RESP;
    resp->is_dir_ = false;
    rpc_send_blk(client_id, blk, sizeof(ResponseGetInode));
    return;
  }

  // send success resp
  resp->msg_ = MetadataRespMsg::INODE_FOUND;
  resp->is_dir_ = inode_idx.type_ == Type::Directory;
  rpc_send_with_data(client_id, blk, data_blk,
                     resp->is_dir_ ? DIR_INODE_SIZE : FILE_INODE_SIZE);
  return;
}

auto MetadataHandler::handle_add_dentry_req(RequestAddDentry *req) -> void {

  BlockInfo *blk = GlobalEnv::rpc()->get_rpc_req_block();
  auto *resp = reinterpret_cast<ResponseAddDentry *>(blk->addr_);
  resp->type_ = RpcMessageType::RPC_TYPE_ADD_DENTRY_RESP;
  resp->notify_ = req->notify_;
  auto [msg, file_ino] = add_dentry(req->dent_key_, req->dent_);
  resp->msg_ = msg;
  resp->exist_ino_ = file_ino;
  trace("in handle_add_dentry_req(), resp->msg_ : {}", (uint32_t)resp->msg_);
  rpc_send_blk(req->client_id_, blk, sizeof(ResponseAddDentry));
  return;
}

auto MetadataHandler::handle_create_inode_req(RequestCreateInode *req) -> void {

  uint32_t client_id = req->client_id_;
  ino_type ino = req->ino_;
  BlockInfo *blk = GlobalEnv::rpc()->get_rpc_req_block();
  auto *resp = reinterpret_cast<ResponseCreateInode *>(blk->addr_);
  resp->type_ = RpcMessageType::RPC_TYPE_CREATE_INODE_RESP;
  resp->notify_ = req->notify_;

  auto r = create_inode(ino, req->is_dir_);
  resp->msg_ = r;
  rpc_send_blk(client_id, blk, sizeof(ResponseCreateInode));
  return;
}

auto MetadataHandler::handle_remove_dentry_req(RequestRemoveDentry *req)
    -> void {
  BlockInfo *blk = GlobalEnv::rpc()->get_rpc_req_block();
  auto *resp = reinterpret_cast<ResponseRemoveDentry *>(blk->addr_);
  resp->type_ = RpcMessageType::RPC_TYPE_REMOVE_DENTRY_RESP;
  resp->notify_ = req->notify_;

  auto [r, old_dent] = remove_dentry(req->dent_key_);
  resp->msg_ = r;
  resp->dent_ = old_dent;
  rpc_send_blk(req->client_id_, blk, sizeof(ResponseRemoveDentry));
  return;
}

auto MetadataHandler::handle_write_req(RequestWrite *req, void *data) -> void {

  uint32_t client_id = req->client_id_;
  btable_key block = {req->file_ino_, req->block_num_};

  BlockInfo *blk = GlobalEnv::rpc()->get_rpc_req_block();
  auto *resp = reinterpret_cast<ResponseWrite *>(blk->addr_);
  resp->type_ = RpcMessageType::RPC_TYPE_WRITE_RESP;
  resp->notify_ = req->notify_;

  auto [r, write_size] = write_block(req->file_ino_, req->block_num_, data,
                                     req->offset_, req->data_size_);
  if (r != MetadataRespMsg::WRITE_SUCCESS) {
    resp->msg_ = r;
    size_t size = sizeof(ResponseWrite);
    rpc_send_blk(client_id, blk, size);
    return;
  }

  // success resp
  resp->msg_ = MetadataRespMsg::WRITE_SUCCESS;
  resp->write_size_ = write_size;
  // resp->target_block_size_ = blk_info_new.size_;
  size_t msg_size = sizeof(ResponseWrite);

  rpc_send_blk(client_id, blk, msg_size);
  return;
};

auto MetadataHandler::handle_inline_write_req(RequestInlineWrite *req,
                                              void *data) -> void {
  uint32_t client_id = req->client_id_;
  BlockInfo *blk = GlobalEnv::rpc()->get_rpc_req_block();
  auto *resp = reinterpret_cast<ResponseInlineWrite *>(blk->addr_);
  resp->type_ = RpcMessageType::RPC_TYPE_INLINE_WRITE_RESP;
  resp->notify_ = req->notify_;
  size_t msg_size = sizeof(RequestInlineWrite);

  auto [r, inline_data_size] =
      inline_write_block(req->file_ino_, req->offset_, data, req->data_size_);
  if (r != MetadataRespMsg::INLINE_WRITE_SUCCESS) {
    resp->msg_ = r;
    rpc_send_blk(client_id, blk, msg_size);
    return;
  }
  resp->msg_ = MetadataRespMsg::INLINE_WRITE_SUCCESS;
  resp->write_size_ = req->data_size_;
  resp->inline_data_size_ = inline_data_size;
  rpc_send_blk(client_id, blk, msg_size);
};

auto MetadataHandler::handle_inline_data_migrate_req(
    RequestInlineDataMigrate *req) -> void {
  uint32_t client_id = req->client_id_;
  BlockInfo *blk = GlobalEnv::rpc()->get_rpc_req_block();

  auto data_blk = GlobalEnv::block_pool()->alloc<void>().value();
  auto *file_inode = reinterpret_cast<FileInode *>(data_blk.second);

  auto *resp = reinterpret_cast<ResponseInlineDataMigrate *>(blk->addr_);
  size_t msg_size = sizeof(ResponseInlineDataMigrate);
  resp->type_ = RpcMessageType::RPC_TYPE_INLINE_DATA_MIGRATE_RESP;
  resp->notify_ = req->notify_;

  auto r = inline_block_migrate(req->file_ino_, file_inode);
  if (r != MetadataRespMsg::INLINE_DATA_MIGRATE_SUCCESS &&
      r != MetadataRespMsg::INLINE_DATA_MIGRATE_ATABLE_ALREADY_EXISTS) {
    resp->msg_ = r;
    rpc_send_blk(client_id, blk, msg_size);
    return;
  }

  resp->msg_ = r;
  rpc_send_with_data(client_id, blk, data_blk, FILE_INODE_SIZE);
  return;
};

auto MetadataHandler::handle_update_inode_req(RequestUpdateInode *req,
                                              void *data) -> void {

  uint32_t client_id = req->client_id_;
  BlockInfo *blk = GlobalEnv::rpc()->get_rpc_req_block();

  auto *resp = reinterpret_cast<ResponseUpdateInode *>(blk->addr_);
  resp->type_ = RpcMessageType::RPC_TYPE_UPDATE_INODE_RESP;
  resp->notify_ = req->notify_;

  ino_idx inode_idx;
  FileInode file_inode;
  ssize_t write_size;
  {
    uint64_t ts = get_timestamp();
    std::lock_guard<std::mutex> lock(
        ino_locks_[req->file_ino_.hash() % ino_locks_cnt]);
    auto r = read_inode(req->file_ino_, inode_idx, &file_inode,
                        req->block_cnt_ == 0);
    if (r != MetadataRespMsg::INODE_FOUND) {
      resp->msg_ = MetadataRespMsg::INODE_NOT_IN_THIS_NODE;
      rpc_send_blk(client_id, blk, sizeof(ResponseUpdateInode));
      return;
    }

    file_inode.access_time_ = ts;
    file_inode.modify_time_ = ts;
    file_inode.file_size_ = std::max(file_inode.file_size_, req->file_size_);

    auto *blocks_ = reinterpret_cast<uint32_t *>(data);
    for (size_t i = 0; i < req->block_cnt_; i++) {
      uint32_t bno = *(blocks_ + 2 * i);
      uint32_t node_id = *(blocks_ + 2 * i + 1);
      file_inode.cast_ATable()->map_[bno] = node_id;
    }
    write_size =
        write_inode(&file_inode, file_inode_fd_, inode_idx.offset_,
                    req->block_cnt_ == 0 ? inode_meta_size : FILE_INODE_SIZE);
  }

  if (write_size != -1) {
    // success
    resp->msg_ = MetadataRespMsg::UPDATE_INODE_SUCCESS;
    size_t msg_size = sizeof(ResponseUpdateInode);
    rpc_send_blk(client_id, blk, msg_size);
  } else {
    // failed
    resp->msg_ = MetadataRespMsg::UPDATE_INODE_FAILED;
    size_t msg_size = sizeof(ResponseUpdateInode);
    rpc_send_blk(client_id, blk, msg_size);
  }
};

auto MetadataHandler::handle_read_req(RequestRead *req) -> void {
  BlockInfo *blk = GlobalEnv::rpc()->get_rpc_req_block();
  auto *resp = reinterpret_cast<ResponseRead *>(blk->addr_);

  auto data_blk = GlobalEnv::block_pool()->alloc<void>().value();
  void *data = reinterpret_cast<void *>(data_blk.second);

  resp->type_ = RpcMessageType::RPC_TYPE_READ_RESP;
  resp->notify_ = req->notify_;

  auto [r, read_size] = read_block(req->file_ino_, req->block_num_, data,
                                   req->offset_, req->read_size_);
  if (r != MetadataRespMsg::READ_SUCCESS) {
    resp->msg_ = r;
    size_t msg_size = sizeof(ResponseRead);
    rpc_send_blk(req->client_id_, blk, msg_size);
    return;
  }

  // success
  resp->msg_ = MetadataRespMsg::READ_SUCCESS;
  resp->read_size_ = read_size;

  // resp
  rpc_send_with_data(req->client_id_, blk, data_blk, read_size);
  return;
};

auto MetadataHandler::handle_delete_dentry_req(RequestDeleteDentry *req)
    -> ino_type {
  dentry_key dent_key;
  dent_key.ino_ = req->parent_ino_;
  strcpy(dent_key.subdir_name_, req->file_name_);
  // dentry dent;
  auto [r, dent] = remove_dentry(dent_key);
  if (r != MetadataRespMsg::REMOVE_DENTRY_SUCCESS) {
    if (req->client_id_ != GlobalEnv::node_id()) {
      BlockInfo *blk_resp = GlobalEnv::rpc()->get_rpc_req_block();
      auto *resp_del =
          reinterpret_cast<ResponseDeleteDentry *>(blk_resp->addr_);
      resp_del->type_ = RpcMessageType::RPC_TYPE_DELETE_DENTRY_RESP;
      resp_del->msg_ = MetadataRespMsg::DELETE_DENTRY_FAILED;
      resp_del->notify_ = req->notify_;
      size_t size = sizeof(ResponseDeleteDentry);
      rpc_send_blk(req->client_id_, blk_resp, size);
    }
    return {};
  }

  // send success resp
  if (req->client_id_ != GlobalEnv::node_id()) {
    BlockInfo *blk_resp = GlobalEnv::rpc()->get_rpc_req_block();
    auto *resp_del = reinterpret_cast<ResponseDeleteDentry *>(blk_resp->addr_);
    resp_del->type_ = RpcMessageType::RPC_TYPE_DELETE_DENTRY_RESP;
    resp_del->msg_ = MetadataRespMsg::DELETE_DENTRY_SUCCESS;
    resp_del->file_ino_ = dent.sub_ino_;
    resp_del->notify_ = req->notify_;
    size_t size = sizeof(ResponseDeleteDentry);
    rpc_send_blk(req->client_id_, blk_resp, size);
    return {};
  }
  return dent.sub_ino_;
};

auto MetadataHandler::handle_delete_inode_req(RequestDeleteInode *req) -> void {
  ino_type file_ino = req->file_ino_;
  ino_idx file_ino_idx;

  BlockInfo *blk_resp = GlobalEnv::rpc()->get_rpc_req_block();
  auto *resp = reinterpret_cast<ResponseDeleteInode *>(blk_resp->addr_);
  resp->type_ = RpcMessageType::RPC_TYPE_DELETE_INODE_RESP;
  resp->notify_ = req->notify_;
  size_t resp_size = sizeof(ResponseDeleteInode);
  // only thread that delete dentry can reach here

  Status s = meta_db_->get_meta(file_ino, &file_ino_idx);
  if (!s.ok()) {
    if (req->client_id_ != GlobalEnv::node_id()) {
      resp->msg_ = MetadataRespMsg::DELETE_INODE_SUCCESS;
      rpc_send_blk(req->client_id_, blk_resp, resp_size);
    } else {
      GlobalEnv::rpc()->memory_pool()->put_one_block(blk_resp);
    }
    return;
  }

  // delete ino_idx in meta_db
  s = meta_db_->delete_meta(file_ino);

  if (req->client_id_ != GlobalEnv::node_id()) {
    resp->msg_ = MetadataRespMsg::DELETE_INODE_SUCCESS;
    rpc_send_blk(req->client_id_, blk_resp, resp_size);
  } else {
    GlobalEnv::rpc()->memory_pool()->put_one_block(blk_resp);
  }

  // delete inode in ext4
  if (file_ino_idx.type_ == Type::Directory) {
    // clear inode block of dir
    // char *null_data = (char *)malloc(DIR_INODE_SIZE);
    // for (size_t i = 0; i < DIR_INODE_SIZE; i++) {
    //   null_data[i] = '\0';
    // }
    // write_ext4(file_inode_fd_, file_ino_idx.offset_, null_data,
    // DIR_INODE_SIZE); free(null_data); add the offset of this inode to idx
    // free list
    dir_inode_offset_free_list_.put_one_free_idx(file_ino_idx.offset_);

  } else if (file_ino_idx.type_ == Type::File) {
    // get A Table
    FileInode inode;
    read_inode(&inode, file_inode_fd_, file_ino_idx.offset_);

    // clear inode block of file
    // string storage_file = GlobalEnv::config().file_inode_path_;
    // for (size_t i = 0; i < FILE_INODE_SIZE; i++) {
    //   write_ext4(file_inode_fd_, file_ino_idx.offset_ + i, (void *)'\0', 1);
    // }

    // add the offset of this inode to idx free list
    file_inode_offset_free_list_.put_one_free_idx(file_ino_idx.offset_);

    // send RequestDeleteData to every data nodes
    size_t file_max_block = inode.file_size_ / GlobalEnv::config().block_size_;
    if (inode.has_atable_) {
      auto *a_table = inode.cast_ATable();
      for (size_t i = 0; i < max_block_count && i < file_max_block; i++) {
        uint32_t node = a_table->map_[i];
        if (node == 0)
          continue;
        BlockInfo *blk = GlobalEnv::rpc()->get_rpc_req_block();
        auto *req_delete_data =
            reinterpret_cast<RequestDeleteData *>(blk->addr_);
        req_delete_data->type_ = RpcMessageType::RPC_TYPE_DELETE_DATA_REQ;
        req_delete_data->file_ino_ = req->file_ino_;
        req_delete_data->block_num_ = i;
        size_t msg_size = sizeof(RequestDeleteData);
        if (node == GlobalEnv::node_id()) {
          handle_delete_data_req(req_delete_data);
          GlobalEnv::rpc()->memory_pool()->put_one_block(blk);
        } else {
          rpc_send_blk(node, blk, msg_size);
        }
      }
      // TODO: remove overflow A table and related data blocks
    }
  }
}

auto MetadataHandler::handle_delete_data_req(RequestDeleteData *req) -> void {
  btable_key block_key = {req->file_ino_, req->block_num_};
  btable_item block_info;
  Status s = btable_db_->get_block_info(block_key, &block_info);
  if (!s.ok()) {
    return;
  }

  // delete ino_idx in btable_db
  s = btable_db_->delete_block_info(block_key);

  // clear data block in ext4 file
  // // clear data block of file
  // string storage_file = GlobalEnv::instance().data_block_path_;
  // for (size_t i = 0; i < FILE_INODE_SIZE; i++) {
  //   write_ext4(GlobalEnv::metadata_handler()->file_inode_fd_,
  //              file_ino_idx.offset_ + i, (void *)'\0', 1);
  // }
  data_block_mgr_->free_block(block_info);
}

auto MetadataHandler::handle_add_overflow_atable_entry_req(
    RequestAddOverflowATableEntry *req) -> void {
  BlockInfo *blk = GlobalEnv::rpc()->get_rpc_req_block();
  auto *resp = reinterpret_cast<ResponseAddOverflowATableEntry *>(blk->addr_);
  resp->type_ = RpcMessageType::RPC_TYPE_ADD_OVERFLOW_ATABLE_ENTRY_RESP;
  resp->notify_ = req->notify_;
  size_t msg_size = sizeof(ResponseAddOverflowATableEntry);

  overflow_atable_key atable_key = {req->file_ino_, req->block_num_};
  overflow_atable_item atable_item = {req->node_id_};

  Status s = overflow_atable_db_->put_entry(atable_key, atable_item);
  if (!s.ok()) {
    resp->msg_ = MetadataRespMsg::ADD_OVERFLOW_ATABLE_ENTRY_FAILED;
    rpc_send_blk(req->client_id_, blk, msg_size);
    return;
  }

  resp->msg_ = MetadataRespMsg::ADD_OVERFLOW_ATABLE_ENTRY_SUCCESS;
  rpc_send_blk(req->client_id_, blk, msg_size);
  return;
}

auto MetadataHandler::handle_get_overflow_atable_entry_req(
    RequestGetOverflowATableEntry *req) -> void {
  BlockInfo *blk = GlobalEnv::rpc()->get_rpc_req_block();
  auto *resp = reinterpret_cast<ResponseGetOverflowATableEntry *>(blk->addr_);
  resp->type_ = RpcMessageType::RPC_TYPE_GET_OVERFLOW_ATABLE_ENTRY_RESP;
  resp->notify_ = req->notify_;
  size_t msg_size = sizeof(ResponseGetOverflowATableEntry);

  overflow_atable_key atable_key = {req->file_ino_, req->block_num_};

  overflow_atable_item atable_item;
  Status s = overflow_atable_db_->get_entry(atable_key, &atable_item);
  if (!s.ok()) {
    resp->msg_ = MetadataRespMsg::GET_OVERFLOW_ATABLE_ENTRY_FAILED;
    resp->node_id_ = -1;
    rpc_send_blk(req->client_id_, blk, msg_size);
    return;
  }

  resp->msg_ = MetadataRespMsg::GET_OVERFLOW_ATABLE_ENTRY_SUCCESS;
  resp->node_id_ = atable_item.node_id_;
  rpc_send_blk(req->client_id_, blk, msg_size);
  return;
}

} // namespace nextfs