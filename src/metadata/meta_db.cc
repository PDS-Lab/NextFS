#include "metadata/meta_db.hh"
#include "metadata/util/coding.hh"

#include "bitset"
#include "iostream"

namespace nextfs {

MetaDB::MetaDB(const Options &options, const string &db_path) {
  Status s = DB::Open(options, db_path, &db_);
  assert(s.ok());
};

MetaDB::~MetaDB() { delete db_; };

auto MetaDB::put_meta(ino_type ino, ino_idx inode_idx) -> Status {
  string key = ino_type_serialize(ino);
  string value = ino_idx_serialize(inode_idx);

  return db_->Put(WriteOptions(), key, value);
};

auto MetaDB::get_meta(ino_type ino, ino_idx *inode_idx) -> Status {
  string key = ino_type_serialize(ino);
  string value;
  Status s = db_->Get(ReadOptions(), key, &value);
  if (s.ok()) {
    *inode_idx = ino_idx_deserialize(value);
  }
  return s;
};

auto MetaDB::delete_meta(ino_type ino) -> Status {
  string key = ino_type_serialize(ino);
  Status s = db_->Delete(WriteOptions(), key);
  return s;
};

} // namespace nextfs
