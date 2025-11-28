#pragma once
#include "ankerl/unordered_dense.h"
#include "common/util.hh"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "string"
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <shared_mutex>

namespace nextfs {

using rocksdb::DB;
using rocksdb::Options;
using rocksdb::ReadOptions;
using rocksdb::Slice;
using rocksdb::Status;
using rocksdb::WriteBatch;
using rocksdb::WriteOptions;

using std::string;

class DentryDB {
public:
  DentryDB(const Options &options, const string &db_path);
  ~DentryDB();

  auto put_dentry(const dentry_key &dent_key, dentry dent) -> Status;
  auto get_dentry(const dentry_key &dent_key, dentry *dent) -> Status;
  auto delete_dentry(const dentry_key &dent_key) -> Status;

private:
  DB *db_;
};

class DentryCache {
public:
  struct dentry_key_hash {
    using is_avalanching = void;
    auto operator()(const dentry_key &key) const -> size_t {
      using namespace ankerl::unordered_dense::detail;
      size_t str_len = strlen(key.subdir_name_);
      uint64_t b = wyhash::hash(key.subdir_name_, str_len);
      return wyhash::mix(key.ino_.hash(), b);
    }
  };

  struct dentry_key_eq {
    auto operator()(const dentry_key &lhs, const dentry_key &rhs) const
        -> bool {
      return lhs.ino_ == rhs.ino_ &&
             0 == strncmp(lhs.subdir_name_, rhs.subdir_name_, max_str_len);
    }
  };

private:
  struct CacheEle {
    ino_type ino_;
    uint64_t timestamp_;
  };
  static constexpr size_t N = 64;
  uint64_t valid_time_ns_ = 1'000'000'000;
  std::shared_mutex rwlock_[N]{};
  ankerl::unordered_dense::map<dentry_key, CacheEle, dentry_key_hash,
                               dentry_key_eq>
      cache_[N]{};

public:
  DentryCache(uint64_t valid_time_ns) : valid_time_ns_(valid_time_ns) {
    for (auto &c : cache_)
      c.reserve(10'000);
  };
  ~DentryCache() = default;
  auto put_dentry(const dentry_key &dent_key, ino_type ino) -> void;
  auto get_dentry(const dentry_key &dent_key, ino_type *ino) -> bool;
  auto delete_dentry(const dentry_key &dent_key) -> void;
};

} // namespace nextfs
