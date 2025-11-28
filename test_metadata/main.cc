#include "iostream"
#include "metadata/args.hh"
#include "metadata/dentry_db.hh"
#include "metadata/meta_db.hh"
#include "metadata/metadata.hh"
#include "metadata/util/idx_free_list.hh"
#include <cstdint>
#include <utility>

namespace nextfs {

using std::cout;
using std::endl;

void db_single_test() {
  info("====== DB Single Test Start ======");

  MetadataHandler *metadata = GlobalEnv::metadata_handler();

  ino_type ino;
  ino.ino_high_ = 2;
  ino.ino_low_ = 11;
  ino.group_id_ = 0;
  ino.server_id_ = 11;

  ino_idx idx{Type::File, 1024};

  Status s = metadata->get_meta_db()->put_meta(ino, idx);
  assert(s.ok());

  ino_idx value;
  s = metadata->get_meta_db()->get_meta(ino, &value);
  assert(s.ok());

  cout << "MetaDB get: " << endl;
  cout << (uint16_t)value.type_ << endl;
  cout << value.offset_ << endl;

  dentry_key dent_key{ino, "subdir"};

  ino_type child_ino;
  child_ino.ino_high_ = 16747;
  child_ino.ino_low_ = 2097152;
  child_ino.group_id_ = 0;
  child_ino.server_id_ = 5;

  dentry den{child_ino.group_id(), child_ino.server_id(), child_ino};

  s = metadata->get_dentry_db()->put_dentry(dent_key, den);
  assert(s.ok());

  dentry value2;
  // strcpy(dent_key.subdir_name_, "subdir2");
  s = metadata->get_dentry_db()->get_dentry(dent_key, &value2);
  assert(s.ok());

  cout << "DentryDB get: " << endl;
  cout << value2.sub_ino_.ino_high_ << endl;
  cout << value2.sub_ino_.ino_low_ << endl;
  cout << value2.sub_ino_.group_id_ << endl;
  cout << value2.sub_ino_.server_id_ << endl;

  ino_type ino1{1, 11, 0, 5}, ino2{2, 11, 0, 5};

  btable_key block1{ino1, 0}, block2{ino2, 0};

  btable_item blk_info1 = {22};
  GlobalEnv::metadata_handler()->get_btable_db()->put_block_info(block1,
                                                                 blk_info1);

  btable_item blk_info2{};
  s = GlobalEnv::metadata_handler()->get_btable_db()->get_block_info(
      block2, &blk_info2);
  if (s.ok()) {
    cout << "BTableDB get: " << endl;
    cout << blk_info2.offset_ << endl;
    // cout << blk_info2.size_ << endl;
  } else {
    cout << "BTableDB get nothing " << endl;
  }
  info("====== DB Single Test Over ======");
}

void idx_free_list_test() {
  info("====== Idx Free List Test Start ======");
  IdxFreeList *list = new IdxFreeList();
  list->put_one_free_idx(5);
  list->put_one_free_idx(4);
  list->put_one_free_idx(3);
  list->put_one_free_idx(2);
  list->put_one_free_idx(1);

  info("traverse the list");
  uint32_t length = list->length();
  info("length : {}", length);
  for (size_t i = 0; i < length; i++) {
    std::cout << list->get_one_free_idx() << std::endl;
  }

  info("====== Idx Free List Test Over ======");
}

void parse_path_test(string path) {
  info("====== Parse Path Test Start ======");
  // get the root ino_type
  ParsePathArgs args = {path};
  ParsePathResult ret = GlobalEnv::metadata_handler()->parse_path(args);
  if (ret.msg_ != ResultMsg::PARSE_PATH_SUCCESS) {
    info("parse_path_test failed");
  } else {
    info("parse_path_test success");
    info("target node is : {}", ret.target_ino_.node_id());
  }

  info("====== Parse Path Test Over ======");
}

void create_dir_test(string parent_path, string dir_name) {
  info("====== Create Directory Test Start ======");
  // generate ino_
  ino_type ino_ = Snowflake::next_ino();
  uint32_t hash_node = GlobalEnv::metadata_handler()->hash_server_id(ino_);
  ino_.group_id_ = get_group_id(hash_node);
  ino_.server_id_ = get_server_id(hash_node);
  info("ino_.group_id_ : {}", ino_.group_id_);
  info("ino_.server_id_ : {}", ino_.server_id_);

  CreateArgs create_args;
  create_args.file_name_ = dir_name;
  create_args.is_dir_ = true;
  create_args.ino_ = ino_;

  // get parent ino_
  ParsePathArgs parse_args = {parent_path};
  ParsePathResult parse_ret =
      GlobalEnv::metadata_handler()->parse_path(parse_args);
  if (parse_ret.msg_ != ResultMsg::PARSE_PATH_SUCCESS) {
    info("create directory failed, not find the parent path");
  } else {
    create_args.parent_ino_ = parse_ret.target_ino_;

    CreateResult create_ret =
        GlobalEnv::metadata_handler()->create(create_args);
    if (create_ret.msg_ != ResultMsg::CREATE_SUCCESS) {
      info("create directory faild");
    } else {
      info("dir inode info: ");
      info("create directory success");
    }
  }

  info("====== Create Directory Test Over ======");
}

void create_file_test(string parent_path, string file_name) {
  info("====== Create File Test Start ======");
  // generate ino_
  ino_type ino_ = Snowflake::next_ino();
  uint32_t hash_node = GlobalEnv::metadata_handler()->hash_server_id(ino_);
  ino_.group_id_ = get_group_id(hash_node);
  ino_.server_id_ = get_server_id(hash_node);
  info("ino_.group_id_ : {}", ino_.group_id_);
  info("ino_.server_id_ : {}", ino_.server_id_);

  CreateArgs create_args;
  create_args.file_name_ = file_name;
  create_args.is_dir_ = false;
  create_args.ino_ = ino_;

  // get parent ino_
  ParsePathArgs parse_args = {parent_path};
  ParsePathResult parse_ret =
      GlobalEnv::metadata_handler()->parse_path(parse_args);
  if (parse_ret.msg_ != ResultMsg::PARSE_PATH_SUCCESS) {
    info("create file failed, not find the parent path");
  } else {
    create_args.parent_ino_ = parse_ret.target_ino_;

    CreateResult create_ret =
        GlobalEnv::metadata_handler()->create(create_args);
    if (create_ret.msg_ != ResultMsg::CREATE_SUCCESS) {
      info("create file faild");
    } else {
      info("create file success");
    }
  }

  info("====== Create File Test Over ======");
}

void get_inode_test(string path) {
  info("====== Get Inode Test Start ======");
  // get ino_
  ParsePathArgs parse_args = {path};
  ParsePathResult parse_ret =
      GlobalEnv::metadata_handler()->parse_path(parse_args);
  if (parse_ret.msg_ != ResultMsg::PARSE_PATH_SUCCESS) {
    info("can not get the inode, parse_path failed");
  } else {
    GetInodeArgs args{parse_ret.target_ino_};
    GetInodeResult get_ret;
    GlobalEnv::metadata_handler()->get_inode(args, get_ret);
    if (get_ret.msg_ != ResultMsg::GET_INODE_SUCCESS) {
      info("get inode failed");
    } else {
      info("it's : {}", get_ret.is_dir_ ? "directory" : "file");
      if (get_ret.is_dir_) {
        DirInode dir_inode = get_ret.dir_inode_;
        info("dir inode info: ");
        info("perm : {}", dir_inode.perm_);
        info("create time : {}", dir_inode.create_time_);
        info("access time : {}", dir_inode.access_time_);
        info("modify time : {}", dir_inode.modify_time_);
        info("get inode success");
      } else {
        FileInode file_inode = get_ret.file_inode_;
        info("file inode info : ");
        info("file size : {}", file_inode.file_size_);
        info("perm : {}", file_inode.perm_);
        info("create time : {}", file_inode.create_time_);
        info("access time : {}", file_inode.access_time_);
        info("modify time : {}", file_inode.modify_time_);
        info("has atable ? : {}", file_inode.has_atable_);
        if (file_inode.has_atable_) {
          ATable *a_table = reinterpret_cast<ATable *>(file_inode.inline_data_);
          info("the top five records of ATable : ");
          for (int i = 0; i < 5; i++) {
            info("block num {} -> node <{}> | ", i, a_table->map_[i]);
          }
        } else {
          info("the inline data is : ");
          printf("%.*s\n", (int)file_inode.file_size_, file_inode.inline_data_);
        }

        info("get inode success");
      }
    }
  }
  info("====== Get Inode Test Over ======");
}

void rename_test(string old_path, string new_path) {
  info("====== Rename Test Start ======");
  int pos1 = old_path.rfind('/');
  string old_name = old_path.substr(pos1 + 1);
  int pos2 = new_path.rfind('/');
  string new_name = new_path.substr(pos2 + 1);

  string old_parent = old_path.substr(0, pos1);
  string new_parent = new_path.substr(0, pos2);
  if (pos2 == 0)
    new_parent = "/";
  // info("old_name : {}", old_name);
  // info("new_name : {}", new_name);
  info("old_parent : {}", old_parent);
  info("new_parent : {}", new_parent);

  ino_type old_parent_ino, new_parent_ino;

  ParsePathArgs parse_args{old_parent};
  ParsePathResult parse_ret =
      GlobalEnv::metadata_handler()->parse_path(parse_args);
  if (parse_ret.msg_ != ResultMsg::PARSE_PATH_SUCCESS) {
    info("can not find the old_parent's ino");
    info("====== Rename Test Over ======");
    return;
  } else {
    old_parent_ino = parse_ret.target_ino_;
  }

  parse_args.total_path_ = new_parent;
  parse_ret = GlobalEnv::metadata_handler()->parse_path(parse_args);
  if (parse_ret.msg_ != ResultMsg::PARSE_PATH_SUCCESS) {
    info("can not find the new_parent's ino");
    info("====== Rename Test Over ======");
    return;
  } else {
    new_parent_ino = parse_ret.target_ino_;
  }

  RenameArgs args;
  args.old_parent_ino_ = old_parent_ino;
  args.new_parent_ino_ = new_parent_ino;
  args.old_name_ = old_name;
  args.new_name_ = new_name;

  RenameResult ret = GlobalEnv::metadata_handler()->rename(args);
  if (ret.msg_ != ResultMsg::RENAME_SUCCESS) {
    info("rename failed");
  } else {
    info("rename success");
  }

  info("====== Rename Test Over ======");
}

void outline_write_test(string file_path, string data, uint32_t block_num,
                        uint16_t offset) {
  info("====== Write Test Start ======");
  ino_type file_ino;
  ParsePathArgs parse_args{file_path};
  ParsePathResult parse_ret =
      GlobalEnv::metadata_handler()->parse_path(parse_args);
  if (parse_ret.msg_ != ResultMsg::PARSE_PATH_SUCCESS) {
    info("can not find the file's ino");
    info("====== Write Test Over ======");
    return;
  } else {
    file_ino = parse_ret.target_ino_;
  }

  // get_inode
  GetInodeArgs get_inode_args;
  get_inode_args.file_ino_ = file_ino;
  GetInodeResult get_inode_ret;
  GlobalEnv::metadata_handler()->get_inode(get_inode_args, get_inode_ret);
  if (get_inode_ret.msg_ != ResultMsg::GET_INODE_SUCCESS) {
    info("get_inode failed, err msg is : {}", (uint16_t)get_inode_ret.msg_);
    info("====== Write Test Over ======");
    return;
  }
  ATable *a_table =
      reinterpret_cast<ATable *>(get_inode_ret.file_inode_.inline_data_);
  uint32_t data_node = a_table->map_[block_num] == 0 ? GlobalEnv::node_id()
                                                     : a_table->map_[block_num];
  // write
  WriteArgs write_args{};
  write_args.file_ino_ = file_ino;
  write_args.data_node_id_ = data_node;
  write_args.data_ = (char *)malloc(data.length());
  memcpy(write_args.data_, data.c_str(), data.length());
  write_args.block_num_ = block_num;
  write_args.offset_ = offset;
  write_args.data_size_ = data.length();

  WriteResult write_ret = GlobalEnv::metadata_handler()->write(write_args);
  if (write_ret.msg_ != ResultMsg::WRITE_SUCCESS) {
    info("write failed, err msg is : {}", (uint16_t)write_ret.msg_);
    info("====== Write Test Over ======");
    return;
  }

  // update inode
  UpdateInodeArgs update_inode_args;
  update_inode_args.file_ino_ = file_ino;
  update_inode_args.file_size_ =
      std::max(get_inode_ret.file_inode_.file_size_,
               (uint64_t)(write_args.block_num_ * 4096 + write_args.offset_ +
                          write_ret.write_size_));
  update_inode_args.blocks_.emplace_back(write_args.block_num_,
                                         write_args.data_node_id_);
  UpdateInodeResult update_inode_ret =
      GlobalEnv::metadata_handler()->update_inode(update_inode_args);

  if (update_inode_ret.msg_ != ResultMsg::UPDATE_INODE_SUCCESS) {
    info("update inode failed, err msg is : {}",
         (uint16_t)update_inode_ret.msg_);
    info("====== Write Test Over ======");
    return;
  }

  info("write success");
  info("write_size: {}", write_ret.write_size_);
  // info("target_block_size_: {}", write_ret.target_block_size_);
  info("file size: {}", update_inode_args.file_size_);
  info("====== Write Test Over ======");
}

void inline_write_test(string file_path, string data, uint16_t offset) {
  info("====== Inline Write Test Start ======");
  ino_type file_ino;
  ParsePathArgs parse_args{file_path};
  ParsePathResult parse_ret =
      GlobalEnv::metadata_handler()->parse_path(parse_args);
  if (parse_ret.msg_ != ResultMsg::PARSE_PATH_SUCCESS) {
    info("can not find the file's ino");
    info("====== Inline Write Test Over ======");
    return;
  } else {
    file_ino = parse_ret.target_ino_;
  }

  // get_inode
  GetInodeArgs get_inode_args;
  get_inode_args.file_ino_ = file_ino;
  GetInodeResult get_inode_ret;
  GlobalEnv::metadata_handler()->get_inode(get_inode_args, get_inode_ret);
  if (get_inode_ret.msg_ != ResultMsg::GET_INODE_SUCCESS) {
    info("get_inode failed, err msg is : {}", (uint16_t)get_inode_ret.msg_);
    info("====== Inline Write Test Over ======");
    return;
  }

  if (get_inode_ret.file_inode_.has_atable_) {
    info("inode has atable, can not call inline_write()");
    info("====== Inline Write Test Over ======");
    return;
  }

  InlineWriteArgs inline_write_args;
  inline_write_args.file_ino_ = file_ino;
  inline_write_args.offset_ = offset;
  inline_write_args.data_size_ = data.length();
  inline_write_args.data_ = (char *)malloc(data.length());
  memcpy(inline_write_args.data_, data.c_str(), data.length());

  InlineWriteResult inline_write_ret =
      GlobalEnv::metadata_handler()->inline_write(inline_write_args);
  if (inline_write_ret.msg_ != ResultMsg::INLINE_WRITE_SUCCESS) {
    info("inline_write failed");
  } else {
    info("write_size : {}", inline_write_ret.write_size_);
    info("inline_data_size : {}", inline_write_ret.inline_data_size_);
    info("inline_write success");
  }
  info("====== Inline Write Test Over ======");
}

void inline_data_migrate_test(string file_path) {
  info("====== Inline Data Migrate Test Start ======");
  ino_type file_ino;
  ParsePathArgs parse_args{file_path};
  ParsePathResult parse_ret =
      GlobalEnv::metadata_handler()->parse_path(parse_args);
  if (parse_ret.msg_ != ResultMsg::PARSE_PATH_SUCCESS) {
    info("can not find the file's ino");
    info("====== Inline Data Migrate Test Over ======");
    return;
  } else {
    file_ino = parse_ret.target_ino_;
  }

  InlineDataMigrateArgs inline_data_migrate_args;
  inline_data_migrate_args.file_ino_ = file_ino;
  InlineDataMigrateResult ret;
  GlobalEnv::metadata_handler()->inline_data_migrate(inline_data_migrate_args,
                                                     ret);
  if (ret.msg_ != ResultMsg::INLINE_DATA_MIGRATE_SUCCESS) {
    info("inline data migrate failed");
  } else {
    info("inline data migrate success");
  }

  info("====== Inline Data Migrate Test Over ======");
}

// write from the begining
void append_write_test(string file_path, uint32_t size, char c) {
  info("====== Append Write Test Start ======");
  ino_type file_ino;
  ParsePathArgs parse_args{file_path};
  ParsePathResult parse_ret =
      GlobalEnv::metadata_handler()->parse_path(parse_args);
  if (parse_ret.msg_ != ResultMsg::PARSE_PATH_SUCCESS) {
    info("can not find the file's ino");
    info("====== Append Write Test Over ======");
    return;
  } else {
    file_ino = parse_ret.target_ino_;
  }

  /*
    write in 512 units in inline write,
    write in 4k (block size) uints in ouline write,
  */

  // get_inode
  GetInodeArgs get_inode_args;
  get_inode_args.file_ino_ = file_ino;
  GetInodeResult get_inode_ret;
  GlobalEnv::metadata_handler()->get_inode(get_inode_args, get_inode_ret);
  if (get_inode_ret.msg_ != ResultMsg::GET_INODE_SUCCESS) {
    info("can not get the file's inode");
    info("====== Append Write Test Over ======");
    return;
  }
  FileInode file_inode = get_inode_ret.file_inode_;

  uint64_t remain_size = size;
  uint64_t write_size = 512;
  if (write_size > remain_size)
    write_size = remain_size;

  uint64_t file_size = file_inode.file_size_;
  // inline write
  while (file_size + write_size < inline_data_size_limit && remain_size > 0) {
    InlineWriteArgs inline_write_args;
    char data[write_size];
    memset(data, c, write_size);
    inline_write_args.data_ = data;
    inline_write_args.data_size_ = write_size;
    inline_write_args.file_ino_ = file_ino;
    inline_write_args.offset_ = file_size;
    InlineWriteResult inline_write_ret;
    inline_write_ret =
        GlobalEnv::metadata_handler()->inline_write(inline_write_args);
    if (inline_write_ret.msg_ != ResultMsg::INLINE_WRITE_SUCCESS) {
      info("inline write failed");
      info("====== Append Write Test Over ======");
      return;
    }

    file_size = inline_write_ret.inline_data_size_;
    remain_size = size - file_size;
    if (write_size > remain_size)
      write_size = remain_size;
  }

  if (remain_size <= 0) {
    info("append write success");
    info("====== Append Write Test Over ======");
    return;
  }

  // inline data migrate
  InlineDataMigrateArgs inline_data_migrate_args;
  inline_data_migrate_args.file_ino_ = file_ino;
  InlineDataMigrateResult inline_data_migrate_ret;
  GlobalEnv::metadata_handler()->inline_data_migrate(inline_data_migrate_args,
                                                     inline_data_migrate_ret);
  if (inline_data_migrate_ret.msg_ != ResultMsg::INLINE_DATA_MIGRATE_SUCCESS) {
    info("inline data migrate failed");
    info("====== Append Write Test Over ======");
    return;
  }
  file_inode = inline_data_migrate_ret.file_inode_;
  ATable *a_table = reinterpret_cast<ATable *>(file_inode.inline_data_);

  // outline write
  std::vector<std::pair<uint32_t, uint32_t>> blocks;
  uint32_t start_block_num = 1;
  uint32_t block_cnt = 0;
  uint32_t block_size = GlobalEnv::config().block_size_;
  char data[block_size];
  memset(data, c, block_size);

  bool is_big_file = false;

  while (remain_size > 0) {
    if (file_size < GlobalEnv::config().block_size_) {
      // fill the block_0;
      write_size =
          std::min(GlobalEnv::config().block_size_ - file_size, remain_size);
      char data[write_size];
      memset(data, c, write_size);
      WriteArgs write_args;
      write_args.block_num_ = 0;
      write_args.data_ = data;
      write_args.data_node_id_ = a_table->map_[0];
      write_args.data_size_ = write_size;
      write_args.file_ino_ = file_ino;
      write_args.offset_ = file_size;
      WriteResult write_ret;
      write_ret = GlobalEnv::metadata_handler()->write(write_args);
      if (write_ret.msg_ != ResultMsg::WRITE_SUCCESS) {
        info("outline write failed");
        info("====== Append Write Test Over ======");
        return;
      }
      file_size = write_args.offset_ + write_ret.write_size_;
      remain_size = size - file_size;
      write_size = GlobalEnv::config().block_size_;
      if (write_size > remain_size)
        write_size = remain_size;
      continue;
    }

    // big table
    if (file_size >= max_block_count * GlobalEnv::config().block_size_) {
      is_big_file = true;
      break;
    }

    // write block
    write_size = std::min((uint64_t)block_size, remain_size);
    WriteArgs write_args;
    write_args.block_num_ = start_block_num + block_cnt;
    write_args.data_ = data;
    write_args.data_node_id_ = GlobalEnv::node_id();
    write_args.data_size_ = write_size;
    write_args.file_ino_ = file_ino;
    write_args.offset_ = 0;
    WriteResult write_ret;
    write_ret = GlobalEnv::metadata_handler()->write(write_args);
    if (write_ret.msg_ != ResultMsg::WRITE_SUCCESS) {
      info("outline write failed");
      info("====== Append Write Test Over ======");
      return;
    }
    file_size = write_args.block_num_ * GlobalEnv::config().block_size_ +
                write_args.offset_ + write_ret.write_size_;
    remain_size = size - file_size;

    blocks.emplace_back(write_args.block_num_, write_args.data_node_id_);
    block_cnt++;
  }

  // outline write success,
  // update A table;
  UpdateInodeArgs update_inode_args;
  update_inode_args.file_ino_ = file_ino;
  update_inode_args.file_size_ = file_size;
  update_inode_args.blocks_ = std::move(blocks);
  UpdateInodeResult update_inode_ret;
  update_inode_ret =
      GlobalEnv::metadata_handler()->update_inode(update_inode_args);
  if (update_inode_ret.msg_ != ResultMsg::UPDATE_INODE_SUCCESS) {
    info("update inode failed");
    info("====== Append Write Test Over ======");
    return;
  }

  // big file wirte
  uint32_t block_num = start_block_num + block_cnt;
  while (remain_size > 0 && is_big_file) {
    // write block
    write_size = std::min((uint64_t)block_size, remain_size);
    WriteArgs write_args;
    write_args.block_num_ = block_num;
    write_args.data_ = data;
    write_args.data_node_id_ = GlobalEnv::node_id();
    write_args.data_size_ = write_size;
    write_args.file_ino_ = file_ino;
    write_args.offset_ = 0;
    WriteResult write_ret;
    write_ret = GlobalEnv::metadata_handler()->write(write_args);
    if (write_ret.msg_ != ResultMsg::WRITE_SUCCESS) {
      info("big file write failed");
      info("====== Append Write Test Over ======");
      return;
    }

    // add overflow atable entry
    AddOverflowATableEntryArgs add_atable_args;
    add_atable_args.node_id_ = write_args.data_node_id_;
    add_atable_args.block_num_ = block_num;
    add_atable_args.file_ino_ = file_ino;
    AddOverflowATableEntryResult add_atable_ret =
        GlobalEnv::metadata_handler()->add_overflow_atable_entry(
            add_atable_args);

    if (add_atable_ret.msg_ != ResultMsg::ADD_OVERFLOW_ATABLE_ENTRY_SUCCESS) {
      info("add overflow atable entry failed");
      info("====== Append Write Test Over ======");
      return;
    }

    file_size = write_args.block_num_ * GlobalEnv::config().block_size_ +
                write_args.offset_ + write_ret.write_size_;
    remain_size = size - file_size;
    block_num++;

    // update file_size;
    update_inode_args = {};
    update_inode_args.file_ino_ = file_ino;
    update_inode_args.file_size_ = file_size;
    UpdateInodeResult update_inode_ret;
    update_inode_ret =
        GlobalEnv::metadata_handler()->update_inode(update_inode_args);
    if (update_inode_ret.msg_ != ResultMsg::UPDATE_INODE_SUCCESS) {
      info("update file size failed");
      info("====== Append Write Test Over ======");
      return;
    }
  }

  info("append write success");
  info("====== Append Write Test Over ======");
}

void update_inode_test(string file_path, uint64_t file_size, uint32_t block_num,
                       uint32_t block_cnt, std::vector<uint32_t> &node_ids) {
  info("====== Update Inode Test Start ======");

  ino_type file_ino;
  ParsePathArgs parse_args{file_path};
  ParsePathResult parse_ret =
      GlobalEnv::metadata_handler()->parse_path(parse_args);
  if (parse_ret.msg_ != ResultMsg::PARSE_PATH_SUCCESS) {
    info("can not find the file's ino");
    info("====== Update Inode Test Over ======");
    return;
  } else {
    file_ino = parse_ret.target_ino_;
  }

  UpdateInodeArgs args{};
  args.file_ino_ = file_ino;
  args.file_size_ = file_size;
  for (size_t i = 0; i < block_cnt; i++) {
    args.blocks_.emplace_back(block_num + i, node_ids[i]);
  }

  UpdateInodeResult update_inode_ret =
      GlobalEnv::metadata_handler()->update_inode(args);
  if (update_inode_ret.msg_ != ResultMsg::UPDATE_INODE_SUCCESS) {
    info("update_inode failed, err msg is : {}",
         (uint16_t)update_inode_ret.msg_);
  } else {
    info("update_inode success");
  }

  info("====== Update Inode Test Over ======");
};

void read_test(string file_path, uint32_t block_num, uint32_t offset,
               uint32_t read_size) {
  info("====== Read Test Start ======");

  ino_type file_ino;
  ParsePathArgs parse_args{file_path};
  ParsePathResult parse_ret =
      GlobalEnv::metadata_handler()->parse_path(parse_args);
  if (parse_ret.msg_ != ResultMsg::PARSE_PATH_SUCCESS) {
    info("can not find the file's ino");
    info("====== Read Test Over ======");
    return;
  } else {
    file_ino = parse_ret.target_ino_;
  }

  if (block_num > max_block_count - 1) {
    // big file
    info("block_num > max_block_count - 1, read as big file");
    GetOverflowATableEntryArgs get_atable_args;
    get_atable_args.block_num_ = block_num;
    get_atable_args.file_ino_ = file_ino;

    GetOverflowATableEntryResult get_atable_ret;
    get_atable_ret = GlobalEnv::metadata_handler()->get_overflow_atable_entry(
        get_atable_args);
    if (get_atable_ret.msg_ != ResultMsg::GET_OVERFLOW_ATABLE_ENTRY_SUCCESS) {
      info("can not find the overflow atable entry for this block");
      info("====== Read Test Over ======");
      return;
    }

    ReadArgs args{};
    args.file_ino_ = file_ino;
    args.node_id_ = get_atable_ret.node_id_;
    args.block_num_ = block_num;
    args.offset_ = offset;
    args.read_size_ = read_size;
    // args.data_ = malloc(GlobalEnv::config().block_size_);

    ReadResult read_ret = GlobalEnv::metadata_handler()->read(args);
    if (read_ret.msg_ != ResultMsg::READ_SUCCESS) {
      info("read failed, err msg is : {}", (uint16_t)read_ret.msg_);
    } else {
      info("read success");
      // printf("%s\n", (char *)args.data_);
    }
    // free(args.data_);
    info("====== Read Test Over ======");

  } else {
    // common file
    // get inode
    GetInodeArgs get_inode_args;
    get_inode_args.file_ino_ = file_ino;
    GetInodeResult get_inode_ret;
    GlobalEnv::metadata_handler()->get_inode(get_inode_args, get_inode_ret);
    if (get_inode_ret.msg_ != ResultMsg::GET_INODE_SUCCESS) {
      info("can not get the file's inode");
      info("====== Read Test Over ======");
      return;
    }
    FileInode file_inode = get_inode_ret.file_inode_;
    if (!file_inode.has_atable_) {
      info("small file does not have A table");
      info("====== Read Test Over ======");
      return;
    }

    ATable *a_table = reinterpret_cast<ATable *>(file_inode.inline_data_);
    if (a_table->map_[block_num] == 0) {
      info("A table does not find this block");
      info("====== Read Test Over ======");
      return;
    }

    ReadArgs args{};
    args.file_ino_ = file_ino;
    args.node_id_ = a_table->map_[block_num];
    args.block_num_ = block_num;
    args.offset_ = offset;
    args.read_size_ = read_size;
    // args.data_ = malloc(GlobalEnv::config().block_size_);

    ReadResult read_ret = GlobalEnv::metadata_handler()->read(args);
    if (read_ret.msg_ != ResultMsg::READ_SUCCESS) {
      info("read failed, err msg is : {}", (uint16_t)read_ret.msg_);
    } else {
      info("read success");
      // printf("%s\n", (char *)args.data_);
    }
    // free(args.data_);
    info("====== Read Test Over ======");
  }
}

void delete_test(string file_path) {
  info("====== Delete Test Start ======");
  string parent_path = file_path.substr(0, file_path.rfind('/'));
  if (parent_path.empty()) {
    parent_path = "/";
  }
  ParsePathArgs parse_args{parent_path};
  ParsePathResult parse_ret =
      GlobalEnv::metadata_handler()->parse_path(parse_args);
  if (parse_ret.msg_ != ResultMsg::PARSE_PATH_SUCCESS) {
    info("can not find the parent's ino");
    info("====== Delete Test Over ======");
    return;
  }

  DeleteArgs delete_args;
  delete_args.parent_ino_ = parse_ret.target_ino_;
  delete_args.file_name_ = file_path.substr(file_path.rfind('/') + 1);
  GlobalEnv::metadata_handler()->delete_file(delete_args);

  info("====== Delete Test Over ======");
}

void init_and_connect() {
  int ret = GlobalEnv::init();
  if (ret != -1)
    info("GlobalEnv::init() success");
  else
    cout << "GlobalEnv::init() failed" << endl;

  GlobalEnv::rpc()->run();

  sleep(3); // wait other nodes start

  auto &env = GlobalEnv::instance();
  for (auto &v : env.config().in_group_nodes_) {
    uint32_t id = std::get<0>(v);
    string ip = std::get<1>(v);
    string port = std::get<2>(v);
    if (id <= GlobalEnv::instance().node_id())
      continue;
    env.rpc()->connect(ip.c_str(), port.c_str(), id);
  }
};

// clear db
void clear() {
  string cmd = "rm -rf " + GlobalEnv::config().dentry_path_ + "/*";
  system(cmd.c_str());
  cmd = "rm -rf " + GlobalEnv::config().metadata_path_ + "/*";
  system(cmd.c_str());
  cmd = "rm -rf " + GlobalEnv::config().btable_path_ + "/*";
  system(cmd.c_str());
  cmd = "rm -rf " + GlobalEnv::config().overflow_atable_path_ + "/*";
  system(cmd.c_str());
  cmd = "rm -rf " + GlobalEnv::config().dir_inode_path_;
  system(cmd.c_str());
  cmd = "rm -rf " + GlobalEnv::config().file_inode_path_;
  system(cmd.c_str());
  cmd = "rm -rf " + GlobalEnv::config().data_block_dir_ + "/*";
  system(cmd.c_str());
  info("clear success");
}

} // namespace nextfs

using namespace std;
using namespace nextfs;

void parse_cmd() {
  while (true) {
    string op;
    std::cin >> op;
    if (op == "db_test") {
      db_single_test();
    } else if (op == "idx_free_list_test") {
      idx_free_list_test();
    } else if (op == "parse_path") {
      string path;
      std::cin >> path;
      parse_path_test(path);
    } else if (op == "create_dir") {
      string parent_path, dir_name;
      std::cin >> parent_path >> dir_name;
      create_dir_test(parent_path, dir_name);
    } else if (op == "create_file") {
      string parent_path, file_name;
      std::cin >> parent_path >> file_name;
      create_file_test(parent_path, file_name);
    } else if (op == "get_inode") {
      string path;
      std::cin >> path;
      get_inode_test(path);
    } else if (op == "rename") {
      string old_path, new_path;
      std::cin >> old_path >> new_path;
      rename_test(old_path, new_path);
    } else if (op == "outline_write") {
      string file_path, data;
      uint64_t block_num, offset;
      std::cin >> file_path >> block_num >> offset >> data;
      outline_write_test(file_path, data, block_num, offset);
    } else if (op == "inline_write") {
      string file_path, data;
      uint64_t offset;
      std::cin >> file_path >> offset >> data;
      inline_write_test(file_path, data, offset);
    } else if (op == "inline_data_migrate") {
      string file_path;
      std::cin >> file_path;
      inline_data_migrate_test(file_path);
    } else if (op == "append_write") {
      // write file from the begining.
      string file_path;
      uint32_t size;
      char c;
      std::cin >> file_path >> size >> c;
      append_write_test(file_path, size, c);
    } else if (op == "update_inode") {
      string file_path;
      uint64_t file_size;
      uint32_t block_num, block_cnt;
      std::cin >> file_path >> file_size >> block_num >> block_cnt;
      std::vector<uint32_t> node_ids;
      for (size_t i = 0; i < block_cnt; i++) {
        uint32_t node_id;
        std::cin >> node_id;
        node_ids.push_back(node_id);
      }
      update_inode_test(file_path, file_size, block_num, block_cnt, node_ids);
    } else if (op == "read") {
      string file_path;
      uint32_t node_id, block_num, offset, size;
      std::cin >> file_path >> block_num >> offset >> size;
      read_test(file_path, block_num, offset, size);
    } else if (op == "delete") {
      string file_path;
      std::cin >> file_path;
      delete_test(file_path);
    } else if (op == "clear") {
      clear();
    } else {
      info("invalid cmd");
      continue;
    }
  }
}

int main() {
  init_and_connect();
  parse_cmd();
  return 0;
}

// int main() {
//   int ret = GlobalEnv::init();
//   db_single_test();
// }