//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key = array_[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::MappingAt(int index) -> const MappingType & { return array_[index]; }

/*
 * Helper method to binary search and return the value associated with input "key"
 * range: [0, n-1]
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result,
                                          const KeyComparator &comparator) -> bool {
  int index{};
  auto find = GetKeyIndex(key, &index, comparator);
  if (find) {
    result->resize(0);
    result->push_back(array_[index].second);
    return true;
  }
  return false;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  int insert_index{};
  auto find = GetKeyIndex(key, &insert_index, comparator);
  if (find) {
    return false;
  }
  std::move_backward(array_ + insert_index, array_ + GetSize(), array_ + GetSize() + 1);
  array_[insert_index].first = key;
  array_[insert_index].second = value;
  IncreaseSize(1);
  return true;
}

/**
 *  eg:
 *    this == r1,  recipient == r2
 *    r1->[<invalid, p0>, <1, p1>, <2, p2>, <3, p3>, <4, p4>] ----MoveHalfTo--> r2[]
 *    result: r1->[<invalid, p0>, <1, p1>],r2[<2, p2>, <3, p3>, <4, p4>]
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  assert(recipient != nullptr);
  auto remain_size = GetMinSize();  // equal to split index
  recipient->CopyNFrom(array_ + remain_size, GetSize() - remain_size);
  SetSize(remain_size);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  std::copy(items, items + size, array_ + GetSize());
  IncreaseSize(size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, KeyComparator comparator) {
  int index{};
  auto find = GetKeyIndex(key, &index, comparator);
  if (find) {
    std::move(array_ + index + 1, array_ + GetSize(), array_ + index);
    IncreaseSize(-1);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetKeyIndex(const KeyType &key, int *key_index, const KeyComparator &comparator)
    -> bool {
  auto i = 0;
  auto j = GetSize() - 1;
  while (i <= j) {
    auto m = (j - i) / 2 + i;
    if (comparator(array_[m].first, key) < 0) {
      i = m + 1;
    } else if (comparator(array_[m].first, key) > 0) {
      j = m - 1;
    } else {
      key_index[0] = m;
      return true;
    }
  }
  key_index[0] = i;
  return false;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
