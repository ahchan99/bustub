//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>
#include <vector>

#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

// define page type enum
enum class ModeType { SEARCH = 0, SEARCH_LEFTMOST, SEARCH_RIGHTMOST, INSERT, DELETE };

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;

  // return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // index iterator
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  // print the B+ tree
  void Print(BufferPoolManager *bpm);

  // draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  auto Check() -> bool;

 private:
  template <typename T>
  auto NewPage() -> T *;
  template <typename T>
  auto Split(T *page) -> T *;
  auto GetLeafPage(ModeType mode, const KeyType &key = KeyType(), Transaction *transaction = nullptr) -> LeafPage *;
  auto InsertIntoParent(BPlusTreePage *old_page, BPlusTreePage *new_page, const KeyType &risen_key,
                        Transaction *transaction) -> bool;
  template <typename T>
  auto CoalesceOrRedistribute(T *page, Transaction *transaction) -> bool;
  void UpdateRootPageId(int insert_record = 0);
  template <typename T>
  auto IsSafe(T *page, ModeType mode) -> bool;
  auto IsDirty(ModeType mode) -> bool;

  void Latch(page_id_t page_id, bool is_dirty);
  void Latch(Page *page_id, bool is_dirty);
  void Latch(bool is_dirty, Transaction *transaction = nullptr);
  void Latch(page_id_t page_id, ModeType mode);
  void Latch(Page *page_id, ModeType mode);
  void Latch(ModeType mode, Transaction *transaction = nullptr);
  void ReleaseLatch(page_id_t page_id, bool is_dirty, bool is_success = true);
  void ReleaseLatch(Page *page_id, bool is_dirty, bool is_success = true);
  void ReleaseLatch(page_id_t page_id, ModeType mode, bool is_success = true);
  void ReleaseLatch(Page *page_id, ModeType mode, bool is_success = true);
  void ReleaseLatch(Transaction *transaction, bool is_dirty = false);
  void ReleaseLatch(bool is_dirty);
  void ReleaseLatch(ModeType mode);

  //  auto IsBalanced(page_id_t pid) -> int;
  //  auto IsPageCorr(page_id_t pid, std::pair<KeyType, KeyType> &out) -> bool;

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
  ReaderWriterLatch root_page_id_latch_;
};
}  // namespace bustub
