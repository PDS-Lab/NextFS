#include "metadata/util/coding.hh"
#include "common/logger.hh"
#include "common/util.hh"
#include "iostream"
#include <cstdint>
#include <cstring>
#include <string_view>

namespace nextfs {

template <typename... Args>
auto splice(const Args &...args) -> string {
  string res{};
  ((res += (args + separator)), ...);
  res = res.substr(0, res.length() - 1);
  return res;
}

auto put_fixed16(string &dst, uint16_t value) -> void {
  char buf[sizeof(value)];
  encode_fixed16(buf, value);
  dst.append(buf, sizeof(buf));
};

auto put_fixed32(string &dst, uint32_t value) -> void {
  char buf[sizeof(value)];
  encode_fixed32(buf, value);
  dst.append(buf, sizeof(buf));
};

auto put_fixed64(string &dst, uint64_t value) -> void {
  char buf[sizeof(value)];
  encode_fixed64(buf, value);
  dst.append(buf, sizeof(buf));
};

inline auto encode_fixed16(char *dst, uint16_t value) -> void {
  auto *const buffer = reinterpret_cast<uint8_t *>(dst);

  // Recent clang and gcc optimize this to a single mov / str instruction.
  buffer[0] = static_cast<uint8_t>(value);
  buffer[1] = static_cast<uint8_t>(value >> 8);
};

inline auto encode_fixed32(char *dst, uint32_t value) -> void {
  auto *const buffer = reinterpret_cast<uint8_t *>(dst);

  // Recent clang and gcc optimize this to a single mov / str instruction.
  buffer[0] = static_cast<uint8_t>(value);
  buffer[1] = static_cast<uint8_t>(value >> 8);
  buffer[2] = static_cast<uint8_t>(value >> 16);
  buffer[3] = static_cast<uint8_t>(value >> 24);
};

inline auto encode_fixed64(char *dst, uint64_t value) -> void {
  auto *const buffer = reinterpret_cast<uint8_t *>(dst);

  // Recent clang and gcc optimize this to a single mov / str instruction.
  buffer[0] = static_cast<uint8_t>(value);
  buffer[1] = static_cast<uint8_t>(value >> 8);
  buffer[2] = static_cast<uint8_t>(value >> 16);
  buffer[3] = static_cast<uint8_t>(value >> 24);
  buffer[4] = static_cast<uint8_t>(value >> 32);
  buffer[5] = static_cast<uint8_t>(value >> 40);
  buffer[6] = static_cast<uint8_t>(value >> 48);
  buffer[7] = static_cast<uint8_t>(value >> 56);
};

inline auto decode_fixed16(const char *ptr) -> uint16_t {
  const auto *const buffer = reinterpret_cast<const uint8_t *>(ptr);

  // Recent clang and gcc optimize this to a single mov / ldr instruction.
  return (static_cast<uint32_t>(buffer[0])) |
         (static_cast<uint32_t>(buffer[1]) << 8);
};

inline auto decode_fixed32(const char *ptr) -> uint32_t {
  const auto *const buffer = reinterpret_cast<const uint8_t *>(ptr);

  // Recent clang and gcc optimize this to a single mov / ldr instruction.
  return (static_cast<uint32_t>(buffer[0])) |
         (static_cast<uint32_t>(buffer[1]) << 8) |
         (static_cast<uint32_t>(buffer[2]) << 16) |
         (static_cast<uint32_t>(buffer[3]) << 24);
};

inline auto decode_fixed64(const char *ptr) -> uint64_t {
  const auto *const buffer = reinterpret_cast<const uint8_t *>(ptr);

  // Recent clang and gcc optimize this to a single mov / ldr instruction.
  return (static_cast<uint64_t>(buffer[0])) |
         (static_cast<uint64_t>(buffer[1]) << 8) |
         (static_cast<uint64_t>(buffer[2]) << 16) |
         (static_cast<uint64_t>(buffer[3]) << 24) |
         (static_cast<uint64_t>(buffer[4]) << 32) |
         (static_cast<uint64_t>(buffer[5]) << 40) |
         (static_cast<uint64_t>(buffer[6]) << 48) |
         (static_cast<uint64_t>(buffer[7]) << 56);
};

auto ino_type_serialize(ino_type ino) -> string {
  string buf{};
  buf.reserve(16);
  ino_type_serialize(ino, buf);
  return buf;
};

auto ino_type_serialize(ino_type ino, string &buf) -> void {
  put_fixed64(buf, ino.ino_high_);
  put_fixed32(buf, ino.ino_low_);
  put_fixed16(buf, ino.group_id_);
  put_fixed16(buf, ino.server_id_);
};

auto ino_type_deserialize(string_view str) -> ino_type {
  auto p = str.data();
  return {decode_fixed64(p), decode_fixed32(p + 8), decode_fixed16(p + 12),
          decode_fixed16(p + 14)};
};

auto ino_idx_serialize(ino_idx idx) -> string {
  string buf{};
  buf.reserve(6);
  ino_idx_serialize(idx, buf);
  return buf;
};

auto ino_idx_serialize(ino_idx idx, string &buf) -> void {
  put_fixed16(buf, (uint16_t)idx.type_);
  put_fixed32(buf, idx.offset_);
};

auto ino_idx_deserialize(string_view str) -> ino_idx {
  auto p = str.data();
  return {static_cast<Type>(decode_fixed16(p)), decode_fixed32(p + 2)};
};

auto dentry_key_serialize(const dentry_key &dent_key) -> string {
  string key{};
  key.reserve(16 + max_str_len);
  dentry_key_serialize(dent_key, key);
  return key;
};

auto dentry_key_serialize(const dentry_key &dent_key, string &buf) -> void {
  ino_type_serialize(dent_key.ino_, buf);
  buf.append(dent_key.subdir_name_); // append '\0' terminated string
};

auto dentry_key_deserialize(string_view str, dentry_key &key) -> void {
  key.ino_ = ino_type_deserialize({str.data(), 16});
  memcpy(key.subdir_name_, str.data() + 16, str.size() - 16);
  key.subdir_name_[str.size() - 16] = '\0';
};

auto dentry_serialize(dentry dent) -> string {
  string buf{};
  buf.reserve(24);
  dentry_serialize(dent, buf);
  return buf;
};

auto dentry_serialize(dentry dent, string &buf) -> void {
  put_fixed32(buf, dent.sub_group_);
  put_fixed32(buf, dent.sub_id_);
  ino_type_serialize(dent.sub_ino_, buf);
};

auto dentry_deserialize(string_view str) -> dentry {
  dentry dent{};
  dent.sub_group_ = decode_fixed32(str.data());
  dent.sub_id_ = decode_fixed32(str.data() + 4);
  dent.sub_ino_ = ino_type_deserialize({str.data() + 8, 16});
  return dent;
};

auto btable_key_serialize(btable_key block) -> string {
  string block_num_str{};
  block_num_str.reserve(20);
  btable_key_serialize(block, block_num_str);
  return block_num_str;
};

auto btable_key_serialize(btable_key block, string &buf) -> void {
  ino_type_serialize(block.ino_, buf);
  put_fixed32(buf, block.block_num_);
};

auto btable_key_deserialize(string_view str) -> btable_key {
  return {
      ino_type_deserialize({str.data(), 16}),
      decode_fixed32(str.data() + 16),
  };
};

auto btable_item_serialize(btable_item blk_info) -> string {
  string buf{};
  buf.reserve(8);
  btable_item_serialize(blk_info, buf);
  return buf;
};

auto btable_item_serialize(btable_item blk_info, string &buf) -> void {
  put_fixed64(buf, blk_info.offset_ << 8 | blk_info.file_id_);
};

auto btable_item_deserialize(string_view str) -> btable_item {
  uint64_t raw = decode_fixed64(str.data());
  return {.file_id_ = raw & 0xff, .offset_ = raw >> 8};
};

auto overflow_atable_key_serialize(overflow_atable_key key) -> string {
  string block_num_str{};
  block_num_str.reserve(20);
  overflow_atable_key_serialize(key, block_num_str);
  return block_num_str;
};

auto overflow_atable_key_serialize(overflow_atable_key key, string &buf)
    -> void {
  ino_type_serialize(key.ino_, buf);
  put_fixed32(buf, key.block_num_);
};

auto overflow_atable_key_deserialize(string_view str) -> overflow_atable_key {
  return {
      ino_type_deserialize({str.data(), 16}),
      decode_fixed32(str.data() + 16),
  };
};

auto overflow_atable_item_serialize(overflow_atable_item item) -> string {
  string buf{};
  buf.reserve(4);
  overflow_atable_item_serialize(item, buf);
  return buf;
};

auto overflow_atable_item_serialize(overflow_atable_item item, string &buf)
    -> void {
  put_fixed32(buf, item.node_id_);
};

auto overflow_atable_item_deserialize(string_view str) -> overflow_atable_item {
  return {decode_fixed32(str.data())};
}

} // namespace nextfs
