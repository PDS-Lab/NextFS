#include "metadata/btable_db.hh"
#include "common/logger.hh"
#include "common/util.hh"
#include "metadata/util/coding.hh"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

namespace nextfs {

BTableDB::BTableDB(const Options &options, const string &db_path) {
  Status s = DB::Open(options, db_path, &db_);
  assert(s.ok());
};

BTableDB::~BTableDB() { delete db_; };

auto BTableDB::put_block_info(btable_key block, btable_item blk_info)
    -> Status {
  string key = btable_key_serialize(block);
  string value = btable_item_serialize(blk_info);
  return db_->Put(WriteOptions(), key, value);
}

auto BTableDB::put_block_infos(btable_key *blocks, btable_item *blk_infos,
                               int num_blocks) -> Status {
  if (num_blocks == 0)
    return Status::OK();
  WriteBatch batch;
  string key;
  string value;
  key.reserve(sizeof(btable_key));
  value.reserve(sizeof(btable_item));
  for (int i = 0; i < num_blocks; i++) {
    key.clear();
    value.clear();
    btable_key_serialize(blocks[i], key);
    btable_item_serialize(blk_infos[i], value);
    batch.Put(key, value);
  }
  return db_->Write(WriteOptions(), &batch);
}

auto BTableDB::get_block_info(btable_key block, btable_item *blk_info)
    -> Status {
  string key = btable_key_serialize(block);
  string value;

  Status s = db_->Get(ReadOptions(), key, &value);
  if (s.ok()) {
    *blk_info = btable_item_deserialize(value);
  }
  return s;
}

auto BTableDB::get_block_infos(btable_key *blocks, btable_item *blk_infos,
                               int num_blocks) -> std::vector<Status> {
  std::vector<std::string> keys;
  std::vector<rocksdb::Slice> keys_slice;
  std::vector<std::string> values;
  keys.reserve(num_blocks);
  keys_slice.reserve(num_blocks);
  values.reserve(num_blocks);
  for (int i = 0; i < num_blocks; i++) {
    keys.push_back(btable_key_serialize(blocks[i]));
    keys_slice.emplace_back(keys.back());
  }
  auto s = db_->MultiGet(ReadOptions(), keys_slice, &values);
  for (int i = 0; i < num_blocks; i++) {
    if (s[i].ok()) {
      blk_infos[i] = btable_item_deserialize(values[i]);
    }
  }
  return s;
}

auto BTableDB::delete_block_info(btable_key block) -> Status {
  string key = btable_key_serialize(block);
  Status s = db_->Delete(WriteOptions(), key);
  return s;
}

} // namespace nextfs