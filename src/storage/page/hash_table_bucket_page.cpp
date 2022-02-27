//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

// 扫描桶 得到匹配键对应的值 将值存到result中
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  bool ret = false;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i) && cmp(key, array_[i].first) == 0) {
      result->push_back(array_[i].second);
      ret = true;
    }
  }
  return ret;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  size_t available = -1;
  // 遍历所有位置，找到一个可以插入的位置，并且确定有无完全相同的K/V，有则不插入
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      if(cmp(key, array_[i].first) == 0&& value==array_[i].second]){
        return false; // 有完全相同的K/V
      }
    } else if (available = -1) {
      available = i;
    }
  }

  // 遍历完看看找没找到空位
  if (available == -1) {
    return false;
  }

  // 插入数据
  array_[available] = MappingType(key, value);
  SetOccupied(available);
  SetReadable(available);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  // 直接找，找到了调用RemoveAt即可
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      if (cmp(key, array_[i].first) == 0 && value == array_[i].second) {
        RemoveAt(i);
        return true;
      }
    }
  }
  return false;
}

// 得到桶中索引为bucket_idx处的键
template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

// 得到桶中索引为bucket_idx处的值
template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

// 移除桶中索引为bucket_idx处的键值对
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  // 将其位置的readable设置为0即可
  uint8_t c = static_cast<uint8_t>(readable_[bucket_idx / 8]);
  c &= (~(1 << (bucket_idx % 8)));
  readable_[bucket_idx / 8] = static_cast<char>(c);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  // 使用位运算，判断对应位是否为1
  uint8_t c = static_cast<uint8_t>(occupied_[bucket_idx / 8]);
  return (c & (1 << (bucket_idx % 8))) > 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  // 将occupied对应位设置为1
  uint8_t c = static_cast<uint8_t>(occupied_[bucket_idx / 8]);
  c |= (1 << (bucket_idx % 8));
  occupied_[bucket_idx / 8] = static_cast<char>(c);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  // 使用位运算，判断对应位是否为1
  uint8_t c = static_cast<uint8_t>(readable_[bucket_idx / 8]);
  return (c & (1 << (bucket_idx % 8))) > 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  // 将readable对应位设置为1
  uint8_t c = static_cast<uint8_t>(readable_[bucket_idx / 8]);
  c |= (1 << (bucket_idx % 8));
  readable_[bucket_idx / 8] = static_cast<char>(c);
}

// 判断桶是不是满的
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  u_int8_t mask = 255;
  // 先以char为单位
  size_t i_num = BUCKET_ARRAY_SIZE / 8;
  for (size_t i = 0; i < i_num; i++) {
    uint8_t c = static_cast<uint8_t>(readable_[i]);
    if ((c & mask) != mask) {
      return false;
    }
  }

  // 最后还要看剩余的位
  size_t i_remain = BUCKET_ARRAY_SIZE % 8;
  if (i_remain > 0) {
    uint8_t c = static_cast<uint8_t>(readable_[i_num]);
    for (size_t j = 0; j < i_remain; j++) {
      if ((c & 1) != 1) {
        return false;
      }
      c >>= 1;
    }
  }
  return true;
}

// 返回桶中可读元素个数,即当前桶中元素数量
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  // 要分别对每个char中的每位做判断
  uint32_t num = 0;

  // 先以char为单位
  size_t i_num = BUCKET_ARRAY_SIZE / 8;
  for (size_t i = 0; i < i_num; i++) {
    uint8_t c = static_cast<uint8_t>(readable_[i]);
    for (uint8_t j = 0; j < 8; j++) {
      // 取最低位判断
      if ((c & 1) > 0) {
        num++;
      }
      c >>= 1;
    }
  }

  // 最后还要看剩余的
  size_t i_remain = BUCKET_ARRAY_SIZE % 8;
  if (i_remain > 0) {
    uint8_t c = static_cast<uint8_t>(readable_[i_num]);
    for (size_t j = 0; j < i_remain; j++) {
      // 取最低位判断
      if ((c & 1) == 1) {
        num++;
      }
      c >>= 1;
    }
  }

  return num;
}

// 判断桶是不是空的
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  // readable数组使用char存储，但这里不需要挨个判断，
  // 只需要有其中一个不符合条件(不为空)就可以返回false
  u_int8_t mask = 255;
  for (size_t i = 0; i < sizeof(readable_) / sizeof(readable_[0]); i++) {
    if ((readable_[i] & mask) > 0) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
