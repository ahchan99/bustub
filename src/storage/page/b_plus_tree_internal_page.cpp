//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key = array_[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  ValueType value = array_[index].second;
  return value;
}
/*
 * Helper method to binary search and return the value associated with input "key"
 * Range: [1, n-1]
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetValue(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  auto i = 1;
  auto j = GetSize() - 1;
  while (i <= j) {
    auto m = (j - i) / 2 + i;
    // 小于 key 走左侧，大于等于 key 走右侧
    if (comparator(array_[m].first, key) < 0) {
      i = m + 1;
    } else if (comparator(array_[m].first, key) > 0) {
      j = m - 1;
    } else {
      return array_[m].second;  // mid 为查找的值并入右侧
    }
  }
  return array_[i - 1].second;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const ValueType &old_value, const KeyType &new_key,
                                            const ValueType &new_value) -> bool {
  auto i = 0;
  auto j = GetSize() - 1;
  while (i <= j) {
    auto m = (j - i) / 2 + i;
    if (array_[m].second < old_value) {
      i = m + 1;
    } else if (array_[m].second > old_value) {
      j = m - 1;
    } else {
      auto insert_idx = m + 1;
      std::move_backward(array_ + insert_idx, array_ + GetSize(), array_ + GetSize() + 1);
      array_[insert_idx].first = new_key;
      array_[insert_idx].second = new_value;
      IncreaseSize(1);
      return true;
    }
  }
  return false;
}
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  assert(recipient != nullptr);
  assert(buffer_pool_manager != nullptr);
  auto remain_size = GetMinSize();  // equal to split index
  recipient->CopyNFrom(array_ + remain_size, GetSize() - remain_size, buffer_pool_manager);
  SetSize(remain_size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  std::copy(items, items + size, array_ + GetSize());
  for (int i = 0; i < size; i++) {
    auto page = buffer_pool_manager->FetchPage(ValueAt(i + GetSize()));
    auto *child_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    child_page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
  }
  IncreaseSize(size);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
