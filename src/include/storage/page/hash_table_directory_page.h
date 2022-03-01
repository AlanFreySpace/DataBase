//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_directory_page.h
//
// Identification: src/include/storage/page/hash_table_directory_page.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cassert>
#include <climits>
#include <cstdlib>
#include <string>

#include "storage/index/generic_key.h"
#include "storage/page/hash_table_page_defs.h"

namespace bustub {

#define MAX_BUCKET_DEPTH 9
/**
 *
 * 可扩展散列的目录页
 *
 * 目录项格式 (size in byte):
 * --------------------------------------------------------------------------------------------
 * | LSN (4) | PageId(4) | GlobalDepth(4) | LocalDepths(512) | BucketPageIds(2048) | Free(1524)
 * --------------------------------------------------------------------------------------------
 */
class HashTableDirectoryPage {
 public:
  /**
   * @return 该页的page ID
   */
  page_id_t GetPageId() const;

  /**
   * 设置该页的page ID
   *
   * @param page_id 用来设置page_id_
   */
  void SetPageId(page_id_t page_id);

  /**
   * @return 该页的lsn(日志流水号)
   */
  lsn_t GetLSN() const;

  /**
   * 设置该页的lsn
   *
   * @param lsn the log sequence number to which to set the lsn field
   */
  void SetLSN(lsn_t lsn);

  /**
   * 使用目录索引得到桶的页
   *
   * @param bucket_idx 要寻找的目录索引
   * @return bucket bucket_idx的page_id
   */
  page_id_t GetBucketPageId(uint32_t bucket_idx);

  /**
   * 使用bucket index和page_id更新目录索引
   *
   * @param bucket_idx 要设置page_id的目录索引
   * @param bucket_page_id 要设置的page_id
   */
  void SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id);

  /**
   * split image of an index
   *
   * @param bucket_idx the directory index for which to find the split image
   * @return the directory index of the split image
   **/
  uint32_t GetSplitImageIndex(uint32_t bucket_idx);

  /**
   * GetGlobalDepthMask - 返回一个有global_depth个1其余为0的掩码.
   *
   * 再可扩展哈希中我们使用hash函数和mask函数将key映射为directory index
   *
   * DirectoryIndex = Hash(key) & GLOBAL_DEPTH_MASK
   *
   * 其中GLOBAL_DEPTH_MASK是一个低位有GLOBAL_DEPTH个1的掩码
   * 例如, global depth 3对应的32位掩码是0x00000007
   *
   * @return 低位有GLOBAL_DEPTH个1,其余为0的掩码
   */
  uint32_t GetGlobalDepthMask();

  /**
   * GetLocalDepthMask - 与global depth mask类似, 只不过它返回
   * bucket_idx号桶local depth的掩码
   *
   * @param bucket_idx the index to use for looking up local depth
   * @return mask of local 1's and the rest 0's (with 1's from LSB upwards)
   */
  uint32_t GetLocalDepthMask(uint32_t bucket_idx);

  /**
   * Get the global depth of the hash table directory
   *
   * @return the global depth of the directory
   */
  uint32_t GetGlobalDepth();

  /**
   * Increment the global depth of the directory
   */
  void IncrGlobalDepth();

  /**
   * Decrement the global depth of the directory
   */
  void DecrGlobalDepth();

  /**
   * @return true if the directory can be shrunk
   */
  bool CanShrink();

  /**
   * @return the current directory size
   */
  uint32_t Size();

  /**
   * Gets the local depth of the bucket at bucket_idx
   *
   * @param bucket_idx the bucket index to lookup
   * @return the local depth of the bucket at bucket_idx
   */
  uint32_t GetLocalDepth(uint32_t bucket_idx);

  /**
   * Set the local depth of the bucket at bucket_idx to local_depth
   *
   * @param bucket_idx bucket index to update
   * @param local_depth new local depth
   */
  void SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth);

  /**
   * Increment the local depth of the bucket at bucket_idx
   * @param bucket_idx bucket index to increment
   */
  void IncrLocalDepth(uint32_t bucket_idx);

  /**
   * Decrement the local depth of the bucket at bucket_idx
   * @param bucket_idx bucket index to decrement
   */
  void DecrLocalDepth(uint32_t bucket_idx);

  /**
   * Gets the high bit corresponding to the bucket's local depth.
   * This is not the same as the bucket index itself.  This method
   * is helpful for finding the pair, or "split image", of a bucket.
   *
   * @param bucket_idx bucket index to lookup
   * @return the high bit corresponding to the bucket's local depth
   */
  // uint32_t GetLocalHighBit(uint32_t bucket_idx);

  /**
   * VerifyIntegrity
   *
   * 验证以下条件:
   * (1) 所有的LD <= GD.(Local Depth,Global Depth)
   * (2) 每个桶都有2^(GD - LD)个指针指向它.
   * (3) 指向相同桶的目录索引有着相同的LD
   */
  void VerifyIntegrity();

  /**
   * Prints the current directory
   */
  void PrintDirectory();

 private:
  page_id_t page_id_;
  lsn_t lsn_;
  uint32_t global_depth_{0};
  uint8_t local_depths_[DIRECTORY_ARRAY_SIZE];
  page_id_t bucket_page_ids_[DIRECTORY_ARRAY_SIZE];
};

}  // namespace bustub
