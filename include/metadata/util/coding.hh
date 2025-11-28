#pragma once
#include "common/util.hh"
#include "cstdint"
#include "metadata/meta_db.hh"
#include "string"
#include <string_view>

namespace nextfs {

using std::string;
using std::string_view;

const string separator = "#";

template <typename... Args>
auto splice(const Args &...args) -> string;

auto put_fixed16(string &dst, uint16_t value) -> void;
auto put_fixed32(string &dst, uint32_t value) -> void;
auto put_fixed64(string &dst, uint64_t value) -> void;

inline auto encode_fixed16(char *dst, uint16_t value) -> void;
inline auto encode_fixed32(char *dst, uint32_t value) -> void;
inline auto encode_fixed64(char *dst, uint64_t value) -> void;

inline auto decode_fixed16(const char *ptr) -> uint16_t;
inline auto decode_fixed32(const char *ptr) -> uint32_t;
inline auto decode_fixed64(const char *ptr) -> uint64_t;

auto ino_type_serialize(ino_type ino) -> string;
auto ino_type_serialize(ino_type ino, string &buf) -> void;
auto ino_type_deserialize(string_view str) -> ino_type;

auto ino_idx_serialize(ino_idx idx) -> string;
auto ino_idx_serialize(ino_idx idx, string &buf) -> void;
auto ino_idx_deserialize(string_view str) -> ino_idx;

auto dentry_key_serialize(const dentry_key &dent_key) -> string;
auto dentry_key_serialize(const dentry_key &dent_key, string &buf) -> void;
auto dentry_key_deserialize(string_view str, dentry_key &key) -> void;

auto dentry_serialize(dentry dent) -> string;
auto dentry_serialize(dentry dent, string &buf) -> void;
auto dentry_deserialize(string_view str) -> dentry;

auto btable_key_serialize(btable_key block) -> string;
auto btable_key_serialize(btable_key block, string &buf) -> void;
auto btable_key_deserialize(string_view str) -> btable_key;

auto btable_item_serialize(btable_item blk_info) -> string;
auto btable_item_serialize(btable_item blk_info, string &buf) -> void;
auto btable_item_deserialize(string_view str) -> btable_item;

auto overflow_atable_key_serialize(overflow_atable_key key) -> string;
auto overflow_atable_key_serialize(overflow_atable_key key, string &buf)
    -> void;
auto overflow_atable_key_deserialize(string_view str) -> overflow_atable_key;

auto overflow_atable_item_serialize(overflow_atable_item item) -> string;
auto overflow_atable_item_serialize(overflow_atable_item item, string &buf)
    -> void;
auto overflow_atable_item_deserialize(string_view str) -> overflow_atable_item;

} // namespace nextfs
