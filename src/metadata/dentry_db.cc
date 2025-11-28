#include "metadata/dentry_db.hh"
#include "common/logger.hh"
#include "metadata/util/coding.hh"
#include <cstdint>

namespace nextfs {

DentryDB::DentryDB(const Options &options, const string &db_path) {
  Status s = DB::Open(options, db_path, &db_);
  assert(s.ok());
};

DentryDB::~DentryDB() { delete db_; };

auto DentryDB::put_dentry(const dentry_key &dent_key, dentry dent) -> Status {
  string key = dentry_key_serialize(dent_key);
  string value = dentry_serialize(dent);
  return db_->Put(WriteOptions(), key, value);
};

auto DentryDB::get_dentry(const dentry_key &dent_key, dentry *dent) -> Status {
  string key = dentry_key_serialize(dent_key);
  string value;
  Status s = db_->Get(ReadOptions(), key, &value);
  if (s.ok()) {
    *dent = dentry_deserialize(value);
  }
  return s;
};

auto DentryDB::delete_dentry(const dentry_key &dent_key) -> Status {
  string key = dentry_key_serialize(dent_key);
  Status s = db_->Delete(WriteOptions(), key);
  return s;
};

auto DentryCache::put_dentry(const dentry_key &dent_key, ino_type ino) -> void {
  uint64_t ts = get_timestamp();
  uint64_t shard = dentry_key_hash{}(dent_key) % N;
  {
    std::shared_lock<std::shared_mutex> lock(rwlock_[shard]);
    auto it = cache_[shard].find(dent_key);
    if (it != cache_[shard].end() &&
        ts - it->second.timestamp_ < valid_time_ns_) {
      return;
    }
  }

  std::unique_lock<std::shared_mutex> lock(rwlock_[shard]);
  auto [it, inserted] =
      cache_[shard].insert_or_assign(dent_key, CacheEle{ino, ts});
  if (cache_[shard].size() > 5000) { // random remove expired elements
    it++;
    for (int i = 0; i < 5 && it != cache_[shard].end(); i++) {
      if (ts - it->second.timestamp_ >= valid_time_ns_) {
        it = cache_[shard].erase(it);
      } else {
        it++;
      }
    }
  }
};

auto DentryCache::get_dentry(const dentry_key &dent_key, ino_type *ino)
    -> bool {
  uint64_t ts = get_timestamp();
  uint64_t shard = dentry_key_hash{}(dent_key) % N;
  {
    std::shared_lock<std::shared_mutex> lock(rwlock_[shard]);
    auto it = cache_[shard].find(dent_key);
    if (it == cache_[shard].end()) {
      return false;
    }
    if (ts - it->second.timestamp_ < valid_time_ns_) {
      *ino = it->second.ino_;
      return true;
    }
  }
  // expired, erase it
  std::unique_lock<std::shared_mutex> lock(rwlock_[shard]);
  auto it = cache_[shard].find(dent_key);
  if (it != cache_[shard].end() &&
      ts - it->second.timestamp_ >= valid_time_ns_) {
    cache_[shard].erase(it);
  }
  return false;
}

auto DentryCache::delete_dentry(const dentry_key &dent_key) -> void {
  uint64_t shard = dentry_key_hash{}(dent_key) % N;
  std::unique_lock<std::shared_mutex> lock(rwlock_[shard]);
  cache_[shard].erase(dent_key);
}

} // namespace nextfs