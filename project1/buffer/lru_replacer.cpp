//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) { max_size_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;

/*
依据LRU策略移除最近最少访问的帧
在指针frame_id中写入被删除的id
如果删除成功返回true，否则返回false
*/
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::scoped_lock lock{mutex_};  //申请单个互斥锁
  if (lru_list_.empty()) {
    return false;
  }
  *frame_id = lru_list_.back();
  lru_hash_.erase(lru_list_.back());
  lru_list_.pop_back();
  return true;
}

/*
在将一个页面固定到BufferPoolManager的帧后该方法应该被调用。
该方法将从LRUReplacer中移除该被固定到BufferPoolManager的帧
这样做的目的是:在缓冲池中的帧不应有被LRUReplacer的Victim方法删除的可能
*/
void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock lock{mutex_};
  auto iter = lru_hash_.find(frame_id);
  if (iter == lru_hash_.end()) {
    return;
  }
  lru_list_.erase(iter->second);
  lru_hash_.erase(iter);
}

/*
当一个页面的pin_count变为0后该方法应该被调用。
该方法将在缓冲池中取消固定的帧移入LRUReplacer中。
*/
void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock lock{mutex_};
  if (lru_hash_.count(frame_id) != 0) {  //避免重复添加
    return;
  }
  if (lru_list_.size() >= max_size_) {  //超容量
    return;
  }
  lru_list_.push_front(frame_id);           //添加到链表头
  lru_hash_[frame_id] = lru_list_.begin();  //建立哈希
}

// 该方法返回目前在LRUReplacer中帧的数量。
size_t LRUReplacer::Size() {
  std::scoped_lock lock{mutex_};
  return lru_list_.size();
}

}  // namespace bustub
