//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_header_page.cpp
//
// Identification: src/storage/page/hash_table_header_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_directory_page.h"
#include <algorithm>
#include <unordered_map>
#include "common/logger.h"

namespace bustub {
page_id_t HashTableDirectoryPage::GetPageId() const { return page_id_; }

void HashTableDirectoryPage::SetPageId(bustub::page_id_t page_id) { page_id_ = page_id; }

lsn_t HashTableDirectoryPage::GetLSN() const { return lsn_; }

void HashTableDirectoryPage::SetLSN(lsn_t lsn) { lsn_ = lsn; }

uint32_t HashTableDirectoryPage::GetGlobalDepth() { return global_depth_; }

uint32_t HashTableDirectoryPage::GetGlobalDepthMask() {
  // Example: 当global_depth_是3的时候
  // 0000...000001 << global_depeth_ = 0000...01000
  // 再减1即可
  return ((1 << global_depth_) - 1);
}

void HashTableDirectoryPage::IncrGlobalDepth() {
  // assert(global_depth_ < MAX_BUCKET_DEPTH);
  // 这里主要是需要将bucket_page_ids_和local_depths_的数据在现有数组的末尾再复制一份
  // GlobalDepth加1则目录项增加一倍,但bucket数量不变
  int org_num = Size();
  for (int org_index = 0, new_index = org_num; org_index < org_num; new_index++, org_index++) {
    bucket_page_ids_[new_index] = bucket_page_ids_[org_index];
    local_depths_[new_index] = local_depths_[org_index];
  }
  global_depth_++;
}

void HashTableDirectoryPage::DecrGlobalDepth() { global_depth_--; }

/**
 * 使用目录索引得到桶的页
 *
 * @param bucket_idx 要寻找的目录索引
 * @return bucket bucket_idx的page_id
 */
page_id_t HashTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) { return bucket_page_ids_[bucket_idx]; }

/**
 * 使用bucket index和page_id更新目录索引
 *
 * @param bucket_idx 要设置page_id的目录索引
 * @param bucket_page_id 要设置的page_id
 */
void HashTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

/**
 * @return 当前目录的大小
 */
uint32_t HashTableDirectoryPage::Size() {
  // 2 ^ global_depth_
  return (1 << global_depth_);
}

bool HashTableDirectoryPage::CanShrink() {
  // 整个Directory能不能收缩取决于每个localdepth是否都比globaldepth小
  // 循环判断即可
  for (uint32_t i = 0; i < Size(); i++) {
    if (local_depths_[i] >= global_depth_) {
      return false;
    }
  }
  return true;
}

// 得到第bucket_idx个目录项对应桶的局部深度
uint32_t HashTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) { return local_depths_[bucket_idx]; }

// 设置第bucket_idx个目录项对应桶的局部深度为local_depth
void HashTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  assert(local_depth <= global_depth_);
  local_depths_[bucket_idx] = local_depth;
}

// 将第bucket_idx个目录项对应桶的局部深度加1
void HashTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  assert(local_depths_[bucket_idx] < global_depth_);
  local_depths_[bucket_idx]++;
}

// 将第bucket_idx个目录项对应桶的局部深度减1
void HashTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]--; }

// uint32_t HashTableDirectoryPage::GetLocalHighBit(uint32_t bucket_idx) { return 0; }

uint32_t HashTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) {
  // Example: 当对应的localdepth是3时：
  // 1<<(3-1) = 0000....00100,设bucket_idx=00100 则异或后结果为00000
  // 也就是说目录索引00100和00000同属同一分割镜像
  return bucket_idx ^ (1 << (local_depths_[bucket_idx] - 1));
}

/**
 * VerifyIntegrity - Use this for debugging but **DO NOT CHANGE**
 *
 * If you want to make changes to this, make a new function and extend it.
 *
 * Verify the following invariants:
 * (1) All LD <= GD.
 * (2) Each bucket has precisely 2^(GD - LD) pointers pointing to it.
 * (3) The LD is the same at each index with the same bucket_page_id
 */
void HashTableDirectoryPage::VerifyIntegrity() {
  //  build maps of {bucket_page_id : pointer_count} and {bucket_page_id : local_depth}
  std::unordered_map<page_id_t, uint32_t> page_id_to_count = std::unordered_map<page_id_t, uint32_t>();
  std::unordered_map<page_id_t, uint32_t> page_id_to_ld = std::unordered_map<page_id_t, uint32_t>();

  //  verify for each bucket_page_id, pointer
  for (uint32_t curr_idx = 0; curr_idx < Size(); curr_idx++) {
    page_id_t curr_page_id = bucket_page_ids_[curr_idx];
    uint32_t curr_ld = local_depths_[curr_idx];
    assert(curr_ld <= global_depth_);

    ++page_id_to_count[curr_page_id];

    if (page_id_to_ld.count(curr_page_id) > 0 && curr_ld != page_id_to_ld[curr_page_id]) {
      uint32_t old_ld = page_id_to_ld[curr_page_id];
      LOG_WARN("Verify Integrity: curr_local_depth: %u, old_local_depth %u, for page_id: %u", curr_ld, old_ld,
               curr_page_id);
      PrintDirectory();
      assert(curr_ld == page_id_to_ld[curr_page_id]);
    } else {
      page_id_to_ld[curr_page_id] = curr_ld;
    }
  }

  auto it = page_id_to_count.begin();

  while (it != page_id_to_count.end()) {
    page_id_t curr_page_id = it->first;
    uint32_t curr_count = it->second;
    uint32_t curr_ld = page_id_to_ld[curr_page_id];
    uint32_t required_count = 0x1 << (global_depth_ - curr_ld);

    if (curr_count != required_count) {
      LOG_WARN("Verify Integrity: curr_count: %u, required_count %u, for page_id: %u", curr_ld, required_count,
               curr_page_id);
      PrintDirectory();
      assert(curr_count == required_count);
    }
    it++;
  }
}

void HashTableDirectoryPage::PrintDirectory() {
  LOG_DEBUG("======== DIRECTORY (global_depth_: %u) ========", global_depth_);
  LOG_DEBUG("| bucket_idx | page_id | local_depth |");
  for (uint32_t idx = 0; idx < static_cast<uint32_t>(0x1 << global_depth_); idx++) {
    LOG_DEBUG("|      %u     |     %u     |     %u     |", idx, bucket_page_ids_[idx], local_depths_[idx]);
  }
  LOG_DEBUG("================ END DIRECTORY ================");
}

}  // namespace bustub
