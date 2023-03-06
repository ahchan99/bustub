//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cmath>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.push_back(std::make_shared<Bucket>(bucket_size_, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::shared_mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::shared_lock<std::shared_mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::shared_lock<std::shared_mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::shared_lock<std::shared_mutex> lock(latch_);
  return dir_[IndexOf(key)]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::unique_lock<std::shared_mutex> lock(latch_);
  return dir_[IndexOf(key)]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::unique_lock<std::shared_mutex> lock(latch_);
  InsertInternal(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::InsertInternal(const K &key, const V &value) {
  auto bucket_index = IndexOf(key);
  if (dir_[bucket_index]->Insert(key, value)) {
    return;
  }
  if (dir_[bucket_index]->GetDepth() == global_depth_) {
    int mask = (1 << global_depth_) - 1;
    // increment the global depth and double the size of the directory
    int num_dirs = pow(2.0, global_depth_);
    for (int i = num_dirs; i < num_dirs * 2; i++) {
      auto raw_index = i & mask;
      dir_.push_back(dir_[raw_index]);
    }
    global_depth_++;
  }
  // increment the local depth of the bucket
  dir_[bucket_index]->IncrementDepth();

  // split the bucket
  auto bucket_depth = dir_[bucket_index]->GetDepth();
  int overflow = 1 << (bucket_depth - 1);
  int mask = (1 << bucket_depth) - 1;
  int raw_index = (bucket_index ^ overflow) & mask;
  // create a new bucket
  dir_[raw_index] = std::make_shared<Bucket>(bucket_size_, bucket_depth);
  num_buckets_++;
  for (int i = 1; i <= pow(2, global_depth_ - bucket_depth) - 1; i++) {
    int prefix = i << bucket_depth;
    dir_[raw_index | prefix] = dir_[raw_index];
  }

  // redistribute directory pointers
  auto &items = dir_[bucket_index]->GetItems();
  for (auto item_iter = items.begin(); item_iter != items.end();) {
    auto rehash_index = IndexOf(item_iter->first);
    // overflow bit is different from the current one
    if ((rehash_index & overflow) != (bucket_index & overflow)) {
      dir_[rehash_index]->GetItems().push_back(*item_iter);
      item_iter = items.erase(item_iter);
    } else {
      item_iter++;
    }
  }

  // try to insert again
  InsertInternal(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (auto iter = list_.begin(); iter != list_.end(); iter++) {
    if (iter->first == key) {
      value = iter->second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto iter = list_.begin(); iter != list_.end(); iter++) {
    if (iter->first == key) {
      list_.erase(iter);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // the key is updating an existing pair
  for (auto iter = list_.begin(); iter != list_.end(); iter++) {
    if (iter->first == key) {
      iter->second = value;
      return true;
    }
  }
  // the bucket is full and the key is not updating an existing pair
  if (IsFull()) {
    return false;
  }
  list_.push_back(std::pair<K, V>{key, value});
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
