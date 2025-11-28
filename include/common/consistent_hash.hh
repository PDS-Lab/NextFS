#pragma once

#include "common/logger.hh"
#include <ankerl/unordered_dense.h>
#include <array>
#include <cstddef>
#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "iostream"

namespace nextfs {
class ConsistentHash {
private:
  static inline std::map<uint64_t, uint32_t> v2p_map_;
  static inline std::set<uint32_t> pnodes_;
  static inline int num_of_vnode_per_pnode_;

public:
  ConsistentHash() = default;
  ~ConsistentHash() { v2p_map_.clear(); };
  static auto fnv_hash(std::string key) -> uint64_t {
    const int p = 16777619;
    uint32_t hash = 2166136261;
    for (int idx = 0; idx < key.size(); ++idx) {
      hash = (hash ^ key[idx]) * p;
    }
    hash += hash << 13;
    hash ^= hash >> 7;
    hash += hash << 3;
    hash ^= hash >> 17;
    hash += hash << 5;
    if (hash < 0) {
      hash = -hash;
    }
    return hash;
  }
  static auto init(std::vector<uint32_t> phy_nodes,
                   int num_of_vnode_per_pnode = 2) -> void {
    num_of_vnode_per_pnode_ = num_of_vnode_per_pnode;
    for (auto v : phy_nodes) {
      pnodes_.insert(v);
    }
    for (auto pnode_id : pnodes_) {
      for (int j = 0; j < num_of_vnode_per_pnode_; j++) {
        auto vnode_key = std::to_string(pnode_id) + '#' + char(j + '0');
        v2p_map_.insert({fnv_hash(vnode_key), pnode_id});
      }
    }
  }
  static auto add_pnode(uint32_t pnode_id) -> void {
    pnodes_.insert(pnode_id);
    for (int i = 0; i < num_of_vnode_per_pnode_; i++) {
      auto vnode_key = std::to_string(pnode_id) + '#' + char(i + '0');
      v2p_map_.insert({fnv_hash(vnode_key), pnode_id});
    }
  }
  static auto del_pnode(uint32_t pnode_id) -> void {
    for (int i = 0; i < num_of_vnode_per_pnode_; i++) {
      auto vnode_key = std::to_string(pnode_id) + '#' + char(i + '0');
      auto it = v2p_map_.find(fnv_hash(vnode_key));
      if (it != v2p_map_.end()) {
        v2p_map_.erase(it);
      } else {
        warn("warning: number {} vnode of {} didn't find when del {}", i,
             pnode_id, pnode_id);
      }
    }
  }
  static inline auto get_server_id(const std::string &key) -> uint32_t {
    auto it = v2p_map_.lower_bound(fnv_hash(key));
    if (v2p_map_.empty()) {
      warn("no available node");
      return 0xffff;
    }
    if (it == v2p_map_.end() && !v2p_map_.empty()) {
      return v2p_map_.begin()->second;
    }
    return it->second;
  }
};
} // namespace nextfs