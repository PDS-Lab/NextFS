#pragma once
#include "common/util.hh"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "string"

namespace nextfs {

using rocksdb::DB;
using rocksdb::Options;
using rocksdb::ReadOptions;
using rocksdb::Slice;
using rocksdb::Status;
using rocksdb::WriteBatch;
using rocksdb::WriteOptions;

using std::string;

class MetaDB {
public:
  MetaDB(const Options &options, const string &db_path);
  ~MetaDB();

  auto put_meta(ino_type ino, ino_idx inode_idx) -> Status;
  auto get_meta(ino_type ino, ino_idx *inode_idx) -> Status;
  auto delete_meta(ino_type ino) -> Status;

private:
  DB *db_;

  // lock
  // Spinlock lock_;
};

} // namespace nextfs
