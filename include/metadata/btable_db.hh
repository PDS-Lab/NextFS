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

// the key of B table is btable_block
// the value of B table is btable_offset
class BTableDB {
public:
  BTableDB(const Options &options, const string &db_path);
  ~BTableDB();

  auto put_block_info(btable_key block, btable_item blk_info) -> Status;
  auto put_block_infos(btable_key *blocks, btable_item *blk_infos,
                       int num_blocks) -> Status;
  auto get_block_info(btable_key block, btable_item *blk_info) -> Status;
  auto get_block_infos(btable_key *blocks, btable_item *blk_infos,
                       int num_blocks) -> std::vector<Status>;
  auto delete_block_info(btable_key block) -> Status;

private:
  DB *db_;
};

} // namespace nextfs
