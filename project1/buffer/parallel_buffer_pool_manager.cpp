//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager) {
  // 对这个类进行初始化,创建num_instances个BufferPoolManagerInstances实例
  num_instances_ = num_instances;
  pool_size_ = pool_size;
  next_instance_ = 0;
  for (size_t i = 0; i < num_instances_; i++) {
    managers_.push_back(new BufferPoolManagerInstance(pool_size, num_instances_, i, disk_manager, log_manager));
  }
}

// 析构函数,删除所有BufferPoolManagerInstances对象,回收相应内存
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for (size_t i = 0; i < num_instances_; i++) {
    delete managers_[i];
  }
}

size_t ParallelBufferPoolManager::GetPoolSize() {
  // 得到BufferPoolManagerInstances的容量
  return num_instances_ * pool_size_;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // 得到给定的page id由哪个BufferPoolManager处理.
  return managers_[page_id % num_instances_];
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // 根据page_id从对应的BufferPoolManagerInstance中得到page
  return GetBufferPoolManager(page_id)->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // 根据page_id从对应的BufferPoolManagerInstance中unpin page
  return GetBufferPoolManager(page_id)->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // 根据page_id从对应的BufferPoolManagerInstance刷新page到磁盘
  return GetBufferPoolManager(page_id)->FlushPage(page_id);
}

// 由于要轮询各个Instance，故要加锁处理
Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // 在缓冲池中创建一个新的page. 我们利用round robin的方式从潜在的
  // BufferPoolManagerInstances中请求页的分配
  // 从BufferPoolManagerInstances的起始索引开始, 调用NewPageImpl直到1) 成功
  // 或者 2) 遍历完所有实例也不成功,直接返回nullptr
  std::scoped_lock lock{latch_};
  for (size_t i = 0; i < num_instances_; i++) {
    BufferPoolManager *manager = managers_[next_instance_];
    Page *page = manager->NewPage(page_id);
    next_instance_ = (next_instance_ + 1) % num_instances_;
    if (page != nullptr) {
      return page;
    }
  }
  return nullptr;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // 根据page_id从对应的BufferPoolManagerInstance删除page
  return GetBufferPoolManager(page_id)->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // 刷新所有BufferPoolManagerInstances的页面到磁盘
  for (size_t i = 0; i < num_instances_; i++) {
    managers_[i]->FlushAllPages();
  }
}

}  // namespace bustub
