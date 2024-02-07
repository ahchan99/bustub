/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, Page *page, int index)
    : buffer_pool_manager_(bpm), page_(page), index_(index) {
  if (page != nullptr) {
    leaf_page_ = reinterpret_cast<LeafPage *>(page->GetData());
  } else {
    leaf_page_ = nullptr;
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, LeafPage *leaf_page, int index)
    : buffer_pool_manager_(bpm), leaf_page_(leaf_page), index_(index) {
  if (leaf_page != nullptr) {
    page_ = buffer_pool_manager_->FetchPage(leaf_page_->GetPageId());
    buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false);
  } else {
    page_ = nullptr;
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (leaf_page_ != nullptr) {
    buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return leaf_page_->GetNextPageId() == INVALID_PAGE_ID && index_ == leaf_page_->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return leaf_page_->MappingAt(index_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (index_ == leaf_page_->GetSize() - 1 && leaf_page_->GetNextPageId() != INVALID_PAGE_ID) {
    auto next_page = buffer_pool_manager_->FetchPage(leaf_page_->GetNextPageId());
    buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false);
    page_ = next_page;
    leaf_page_ = reinterpret_cast<LeafPage *>(next_page->GetData());
    index_ = 0;
  } else {
    index_++;
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  return leaf_page_ == nullptr || (leaf_page_->GetPageId() == itr.leaf_page_->GetPageId() && index_ == itr.index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool { return !this->operator==(itr); }

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
