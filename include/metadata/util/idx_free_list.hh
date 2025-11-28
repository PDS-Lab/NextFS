#pragma once
#include <common/spinlock.hh>
#include <iostream>
#include <mutex>
#include <stdint.h>

namespace nextfs {

class IdxFreeNode {
public:
  uint64_t idx_;
  IdxFreeNode *next_;
  IdxFreeNode *prev_;
};

class IdxFreeList {
public:
  IdxFreeList() {
    IdxFreeNode *dummy = free_list_;
    dummy->next_ = nullptr;
    dummy->prev_ = nullptr;
    dummy->idx_ = -1;
  };
  ~IdxFreeList() {
    IdxFreeNode *dummy = free_list_;
    IdxFreeNode *node = dummy->next_;
    while (node != nullptr) {
      IdxFreeNode *tmp = node;
      node = tmp->next_;
      free(tmp);
    }
  };

  auto get_one_free_idx() -> uint64_t {
    std::lock_guard<Spinlock> l(lock_);
    IdxFreeNode *dummy = free_list_;
    IdxFreeNode *node = dummy->next_;
    if (node == nullptr) {
      return -1;
    }
    remove(node);
    return node->idx_;
  };

  auto put_one_free_idx(uint64_t idx) -> void {
    std::lock_guard<Spinlock> l(lock_);
    IdxFreeNode *node = (IdxFreeNode *)malloc(sizeof(IdxFreeNode));
    node->idx_ = idx;
    node->next_ = nullptr;
    node->prev_ = nullptr;
    insert_front(node);
  };

  auto length() -> uint32_t {
    std::lock_guard<Spinlock> l(lock_);
    return length_;
  };

private:
  IdxFreeNode free_list_[1]; // dummy
  uint32_t length_{0};
  Spinlock lock_{};

  auto insert_front(IdxFreeNode *node) -> void {
    IdxFreeNode *dummy = free_list_;
    if (dummy->next_ == nullptr) {
      node->next_ = nullptr;
      node->prev_ = dummy;
      dummy->next_ = node;
    } else {
      dummy->next_->prev_ = node;
      node->next_ = dummy->next_;
      node->prev_ = dummy;
      dummy->next_ = node;
    }
    length_++;
  };

  auto remove(IdxFreeNode *node) -> void {
    if (node->next_ == nullptr) {
      node->prev_->next_ = node->next_;
      node->prev_ = nullptr;
    } else {
      node->prev_->next_ = node->next_;
      node->next_->prev_ = node->prev_;
      node->prev_ = nullptr;
      node->next_ = nullptr;
    }
    length_--;
  };
};

} // namespace nextfs