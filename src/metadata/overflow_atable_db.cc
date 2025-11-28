#include "metadata/overflow_atable_db.hh"
#include "metadata/util/coding.hh"

#include "bitset"

namespace nextfs {
OverflowATableDB::OverflowATableDB(const Options &options,
                                   const string &db_path) {
  Status s = DB::Open(options, db_path, &db_);
  assert(s.ok());
};

OverflowATableDB::~OverflowATableDB() { delete db_; };

auto OverflowATableDB::put_entry(overflow_atable_key key,
                                 overflow_atable_item item) -> Status {
  string key_str = overflow_atable_key_serialize(key);
  string item_str = overflow_atable_item_serialize(item);
  return db_->Put(WriteOptions(), key_str, item_str);
};

auto OverflowATableDB::get_entry(overflow_atable_key key,
                                 overflow_atable_item *item) -> Status {
  string key_str = overflow_atable_key_serialize(key);
  string item_str;
  Status s = db_->Get(ReadOptions(), key_str, &item_str);
  if (s.ok()) {
    *item = overflow_atable_item_deserialize(item_str);
  }
  return s;
}

auto OverflowATableDB::delete_entry(overflow_atable_key key) -> Status {
  string key_str = overflow_atable_key_serialize(key);
  Status s = db_->Delete(WriteOptions(), key_str);
  return s;
}

} // namespace nextfs