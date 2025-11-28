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

class OverflowATableDB {
public:
  OverflowATableDB(const Options &options, const string &db_path);
  ~OverflowATableDB();

  auto put_entry(overflow_atable_key key, overflow_atable_item item) -> Status;
  auto get_entry(overflow_atable_key key, overflow_atable_item *item) -> Status;
  auto delete_entry(overflow_atable_key key) -> Status;

private:
  DB *db_;
};

} // namespace nextfs
