#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  Latch(ModeType::SEARCH);
  if (IsEmpty()) {
    ReleaseLatch(ModeType::SEARCH);
    return false;
  }
  auto leaf = GetLeafPage(ModeType::SEARCH, key, transaction);
  auto ret = leaf->GetValue(key, result, comparator_);
  ReleaseLatch(leaf->GetPageId(), ModeType::SEARCH);
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  Latch(ModeType::INSERT, transaction);
  // New a leaf page as root of b+tree and store the key and value
  if (IsEmpty()) {
    auto root = NewPage<LeafPage>();
    root_page_id_ = root->GetPageId();
    // Call this method everytime root page id is changed.
    UpdateRootPageId();
    // Insert into new leaf page
    auto ret = root->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(root->GetPageId(), true);
    ReleaseLatch(transaction);
    return ret;
  }
  // Insert into leaf page
  auto leaf = GetLeafPage(ModeType::INSERT, key, transaction);
  auto ret = leaf->Insert(key, value, comparator_);
  // Duplicate key
  if (!ret) {
    ReleaseLatch(transaction);
    ReleaseLatch(leaf->GetPageId(), ModeType::INSERT, false);
    return false;
  }
  if (leaf->GetSize() < leaf->GetMaxSize()) {
    ReleaseLatch(transaction);
    ReleaseLatch(leaf->GetPageId(), ModeType::INSERT);
    return true;
  }
  // Splitting condition: number of key/value pairs AFTER insertion equals to max_size for leaf nodes
  // 说明最多是可以容纳 max_size-1 个
  auto new_leaf = Split(leaf);
  auto risen_key = new_leaf->KeyAt(0);
  ret = InsertIntoParent(leaf, new_leaf, risen_key, transaction);
  //  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  ReleaseLatch(leaf->GetPageId(), ModeType::INSERT);
  buffer_pool_manager_->UnpinPage(new_leaf->GetPageId(), true);
  return ret;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  Latch(ModeType::DELETE, transaction);
  if (IsEmpty()) {
    ReleaseLatch(transaction);
    return;
  }
  auto leaf = GetLeafPage(ModeType::DELETE, key, transaction);
  auto size = leaf->GetSize();
  leaf->Remove(key, comparator_);
  if (size == leaf->GetSize()) {
    // buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    ReleaseLatch(transaction);
    ReleaseLatch(leaf->GetPageId(), ModeType::DELETE, false);
    return;
  }
  //  CoalesceOrRedistribute(leaf, transaction);
  //  ReleaseLatch(transaction, true);
  //  ReleaseLatch(leaf->GetPageId(), ModeType::DELETE);
  if (leaf->GetSize() >= leaf->GetMinSize()) {
    ReleaseLatch(transaction);
    ReleaseLatch(leaf->GetPageId(), ModeType::DELETE);
    return;
  }
  // Need Coalesce or redistribute
  auto should_delete = CoalesceOrRedistribute(leaf, transaction);
  if (should_delete) {
    auto page = buffer_pool_manager_->FetchPage(leaf->GetPageId());
    transaction->AddIntoPageSet(page);
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  } else {
    ReleaseLatch(leaf->GetPageId(), ModeType::DELETE);
  }
  ReleaseLatch(transaction, true);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  Latch(ModeType::SEARCH);
  auto leaf = GetLeafPage(ModeType::SEARCH_LEFTMOST);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  Latch(ModeType::SEARCH);
  auto leaf_page = GetLeafPage(ModeType::SEARCH, key);
  int idx{};
  auto ret = leaf_page->GetKeyIndex(key, &idx, comparator_);
  if (ret) {
    return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, idx);
  }
  return INDEXITERATOR_TYPE();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  Latch(ModeType::SEARCH);
  auto leaf = GetLeafPage(ModeType::SEARCH_RIGHTMOST);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf, leaf->GetSize());
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  Latch(ModeType::SEARCH);
  ReleaseLatch(ModeType::SEARCH);
  return root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key{};
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key{};
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

INDEX_TEMPLATE_ARGUMENTS
template <typename T>
auto BPLUSTREE_TYPE::NewPage() -> T * {
  static_assert(std::is_same_v<T, LeafPage> || std::is_same_v<T, InternalPage>);
  page_id_t page_id;
  auto page = buffer_pool_manager_->NewPage(&page_id);
  assert(page != nullptr);
  if (std::is_same_v<T, LeafPage>) {
    auto new_page = reinterpret_cast<LeafPage *>(page->GetData());
    new_page->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);
    return reinterpret_cast<T *>(new_page);
  }
  auto new_page = reinterpret_cast<InternalPage *>(page->GetData());
  new_page->Init(page_id, INVALID_PAGE_ID, internal_max_size_);
  return reinterpret_cast<T *>(new_page);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename T>
auto BPLUSTREE_TYPE::Split(T *page) -> T * {
  static_assert(std::is_same_v<T, LeafPage> || std::is_same_v<T, InternalPage>);
  auto new_page = NewPage<T>();
  if (std::is_same_v<T, LeafPage>) {
    auto leaf = reinterpret_cast<LeafPage *>(page);
    auto new_leaf = reinterpret_cast<LeafPage *>(new_page);
    new_leaf->SetParentPageId(leaf->GetParentPageId());
    leaf->MoveHalfTo(new_leaf);
    new_leaf->SetNextPageId(leaf->GetNextPageId());
    leaf->SetNextPageId(new_leaf->GetPageId());
    return reinterpret_cast<T *>(new_leaf);
  }
  auto internal = reinterpret_cast<InternalPage *>(page);
  auto new_internal = reinterpret_cast<InternalPage *>(new_page);
  new_internal->SetParentPageId(internal->GetParentPageId());
  internal->MoveHalfTo(new_internal, buffer_pool_manager_);
  return reinterpret_cast<T *>(new_internal);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPage(ModeType mode, const KeyType &key, Transaction *transaction) -> BPlusTree::LeafPage * {
  if (IsEmpty()) {
    return nullptr;
  }
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto current = reinterpret_cast<BPlusTreePage *>(page->GetData());
  // Latch crabbing
  auto is_dirty = IsDirty(mode);
  // Not dirty release root_page_id_ latch
  if (!is_dirty) {
    ReleaseLatch(false);
    Latch(page, false);
  } else {
    Latch(page, true);
    if (IsSafe(current, mode)) {
      ReleaseLatch(transaction);
    }
  }
  while (!current->IsLeafPage()) {
    auto internal = reinterpret_cast<InternalPage *>(current);
    page_id_t next_page_id;
    if (mode == ModeType::SEARCH_LEFTMOST) {
      next_page_id = internal->ValueAt(0);
    } else if (mode == ModeType::SEARCH_RIGHTMOST) {
      next_page_id = internal->ValueAt(internal->GetSize() - 1);
    } else {
      next_page_id = internal->GetValue(key, comparator_);
    }
    assert(next_page_id != INVALID_PAGE_ID);
    auto next_page = buffer_pool_manager_->FetchPage(next_page_id);
    auto next = reinterpret_cast<BPlusTreePage *>(next_page->GetData());
    // Latch crabbing
    if (!is_dirty) {
      Latch(next_page, false);
      ReleaseLatch(page, false);
    } else {
      Latch(next_page, true);
      transaction->AddIntoPageSet(page);
      if (IsSafe(next, mode)) {
        ReleaseLatch(transaction);
      }
    }
    page = next_page;
    current = next;
  }
  // now internal_page is a leaf node
  return reinterpret_cast<LeafPage *>(current);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_page, BPlusTreePage *new_page, const KeyType &risen_key,
                                      Transaction *transaction) -> bool {
  if (old_page->IsRootPage()) {
    auto new_root = NewPage<InternalPage>();
    new_root->SetValueAt(0, old_page->GetPageId());
    new_root->SetKeyAt(1, risen_key);
    new_root->SetValueAt(1, new_page->GetPageId());
    new_root->SetSize(2);
    root_page_id_ = new_root->GetPageId();
    UpdateRootPageId();
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    old_page->SetParentPageId(root_page_id_);
    new_page->SetParentPageId(root_page_id_);
    ReleaseLatch(transaction, true);
    return true;
  }
  auto page = buffer_pool_manager_->FetchPage(old_page->GetParentPageId());
  auto parent = reinterpret_cast<InternalPage *>(page->GetData());
  auto ret = parent->Insert(risen_key, new_page->GetPageId(), comparator_);
  if (parent->GetSize() <= internal_max_size_) {
    ReleaseLatch(transaction, true);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    // buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    return ret;
  }
  // Splitting condition: number of children BEFORE insertion equals to max_size for internal nodes
  // 说明最多是可以容纳 max_size 个
  auto new_parent = Split(parent);
  // parent_risen_key 被挪到父节点，等价删除
  auto parent_risen_key = new_parent->KeyAt(0);
  ret = InsertIntoParent(parent, new_parent, parent_risen_key, transaction);
  //  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_parent->GetPageId(), true);
  return ret;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename T>
auto BPLUSTREE_TYPE::CoalesceOrRedistribute(T *page, Transaction *transaction) -> bool {
  static_assert(std::is_same_v<T, LeafPage> || std::is_same_v<T, InternalPage>);
  if (page->GetSize() >= page->GetMinSize()) {
    // ReleaseLatch(transaction, true);
    // ReleaseLatch(page->GetPageId(), ModeType::DELETE);
    // buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    return false;
  }
  if (page->IsRootPage()) {
    if (page->IsLeafPage()) {
      assert(page->GetSize() == 0);
      root_page_id_ = INVALID_PAGE_ID;
      transaction->AddIntoDeletedPageSet(page->GetPageId());
      // buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      // buffer_pool_manager_->DeletePage(page->GetPageId());
      UpdateRootPageId();
      return true;
    }
    if (page->GetSize() == 1) {
      auto root = reinterpret_cast<InternalPage *>(page);
      auto child_page = buffer_pool_manager_->FetchPage(root->ValueAt(0));
      auto new_root = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
      new_root->SetParentPageId(INVALID_PAGE_ID);
      root_page_id_ = new_root->GetPageId();
      UpdateRootPageId();
      buffer_pool_manager_->UnpinPage(new_root->GetPageId(), true);
      transaction->AddIntoDeletedPageSet(page->GetPageId());
      // buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      // buffer_pool_manager_->DeletePage(page->GetPageId());
      return true;
    }
    LOG_DEBUG("Remove logic is error");
  }
  auto fetch_page = buffer_pool_manager_->FetchPage(page->GetParentPageId());
  auto parent = reinterpret_cast<InternalPage *>(fetch_page->GetData());
  int index{};
  auto ret = parent->GetValueIndex(page->GetPageId(), &index);
  if (!ret) {
    LOG_DEBUG("Find child logic is ERROR");
    std::cout << "Parent:" << parent->IsRootPage() << std::endl;
    std::cout << "Find:" << page->GetPageId() << std::endl;
    for (auto i = 0; i < parent->GetSize(); i++) {
      std::cout << parent->ValueAt(i) << " | ";
    }
  }
  std::cout << std::endl;
  // Previous or next child page of parent
  auto from_prev = index != 0;
  auto sibling_index = index + (from_prev ? -1 : 1);
  fetch_page = buffer_pool_manager_->FetchPage(parent->ValueAt(sibling_index));
  Latch(fetch_page, ModeType::DELETE);
  auto sibling = reinterpret_cast<T *>(fetch_page->GetData());
  if (sibling->GetSize() > sibling->GetMinSize()) {
    // Redistribution
    if (page->IsLeafPage()) {
      auto leaf = reinterpret_cast<LeafPage *>(page);
      auto sibling_leaf = reinterpret_cast<LeafPage *>(sibling);
      if (!from_prev) {
        sibling_leaf->MoveFirstToEndOf(leaf);
        parent->SetKeyAt(1, sibling_leaf->KeyAt(0));
      } else {
        sibling_leaf->MoveLastToFrontOf(leaf);
        parent->SetKeyAt(index, leaf->KeyAt(0));
      }
    } else {
      auto internal = reinterpret_cast<InternalPage *>(page);
      auto sibling_internal = reinterpret_cast<InternalPage *>(sibling);
      // internal 需要补充缺省的 key 值
      if (!from_prev) {
        sibling_internal->MoveFirstToEndOf(internal, parent->KeyAt(1), buffer_pool_manager_);
        parent->SetKeyAt(1, sibling_internal->KeyAt(0));
      } else {
        sibling_internal->MoveLastToFrontOf(internal, parent->KeyAt(index), buffer_pool_manager_);
        parent->SetKeyAt(index, internal->KeyAt(0));
      }
    }
    // ReleaseLatch(transaction, true);
    ReleaseLatch(fetch_page, ModeType::DELETE);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
    // buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
    // buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    // buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    return false;
  }
  // Coalesce
  if (page->IsLeafPage()) {
    auto leaf = reinterpret_cast<LeafPage *>(page);
    auto sibling_leaf = reinterpret_cast<LeafPage *>(sibling);
    if (!from_prev) {
      sibling_leaf->MoveAllTo(leaf);
      parent->Remove(1);
    } else {
      leaf->MoveAllTo(sibling_leaf);
      parent->Remove(index);
    }
  } else {
    auto internal = reinterpret_cast<InternalPage *>(page);
    auto sibling_internal = reinterpret_cast<InternalPage *>(sibling);
    if (!from_prev) {
      sibling_internal->MoveAllTo(internal, parent->KeyAt(1), buffer_pool_manager_);
      parent->Remove(1);
    } else {
      internal->MoveAllTo(sibling_internal, parent->KeyAt(index), buffer_pool_manager_);
      parent->Remove(index);
    }
  }
  // buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
  // buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
  bool should_delete = false;
  if (!from_prev) {
    // buffer_pool_manager_->DeletePage(sibling->GetPageId());
    transaction->AddIntoPageSet(fetch_page);
    transaction->AddIntoDeletedPageSet(sibling->GetPageId());
  } else {
    // buffer_pool_manager_->DeletePage(page->GetPageId());
    ReleaseLatch(fetch_page, ModeType::DELETE);
    transaction->AddIntoDeletedPageSet(page->GetPageId());
    should_delete = true;
  }
  // buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  CoalesceOrRedistribute(reinterpret_cast<T *>(parent), transaction);
  return should_delete;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Latch(Page *page, bool is_dirty) {
  if (is_dirty) {
    page->WLatch();
  } else {
    page->RLatch();
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Latch(page_id_t page_id, bool is_dirty) {
  auto page = buffer_pool_manager_->FetchPage(page_id);
  Latch(page, is_dirty);
  buffer_pool_manager_->UnpinPage(page_id, false);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Latch(bool is_dirty, Transaction *transaction) {
  if (is_dirty) {
    assert(transaction != nullptr);
    root_page_id_latch_.WLock();
    transaction->AddIntoPageSet(nullptr);  // nullptr means root_page_id_latch_
  } else {
    root_page_id_latch_.RLock();
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Latch(page_id_t page_id, ModeType mode) { Latch(page_id, IsDirty(mode)); }

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Latch(Page *page, ModeType mode) { Latch(page, IsDirty(mode)); }

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Latch(ModeType mode, Transaction *transaction) { Latch(IsDirty(mode), transaction); }

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseLatch(page_id_t page_id, bool is_dirty, bool is_success) {
  auto page = buffer_pool_manager_->FetchPage(page_id);
  ReleaseLatch(page, is_dirty, is_success);
  buffer_pool_manager_->UnpinPage(page_id, false);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseLatch(Transaction *transaction, bool is_dirty) {
  while (!transaction->GetPageSet()->empty()) {
    auto page = transaction->GetPageSet()->front();
    transaction->GetPageSet()->pop_front();
    if (page == nullptr) {
      root_page_id_latch_.WUnlock();
    } else {
      page->WUnlatch();
      auto page_id = page->GetPageId();
      buffer_pool_manager_->UnpinPage(page_id, is_dirty);
      if (transaction->GetDeletedPageSet()->find(page_id) != transaction->GetDeletedPageSet()->end()) {
        buffer_pool_manager_->DeletePage(page_id);
        transaction->GetDeletedPageSet()->erase(page_id);
      }
    }
  }
  assert(transaction->GetDeletedPageSet()->empty());
  transaction->GetPageSet()->clear();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseLatch(Page *page, bool is_dirty, bool is_success) {
  if (is_dirty) {
    page->WUnlatch();
  } else {
    page->RUnlatch();
  }
  if (!is_success) {
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return;
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), is_dirty);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseLatch(bool is_dirty) {
  if (is_dirty) {
    root_page_id_latch_.WUnlock();
  } else {
    root_page_id_latch_.RUnlock();
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseLatch(Page *page, ModeType mode, bool is_success) {
  ReleaseLatch(page, IsDirty(mode), is_success);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseLatch(page_id_t page_id, ModeType mode, bool is_success) {
  ReleaseLatch(page_id, IsDirty(mode), is_success);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseLatch(ModeType mode) { ReleaseLatch(IsDirty(mode)); }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsDirty(ModeType mode) -> bool {
  return mode != ModeType::SEARCH && mode != ModeType::SEARCH_LEFTMOST && mode != ModeType::SEARCH_RIGHTMOST;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename T>
auto BPLUSTREE_TYPE::IsSafe(T *page, ModeType mode) -> bool {
  static_assert(std::is_same_v<T, LeafPage> || std::is_same_v<T, InternalPage> || std::is_same_v<T, BPlusTreePage>);
  if (std::is_same_v<T, BPlusTreePage>) {
    if (page->IsLeafPage()) {
      return IsSafe(reinterpret_cast<LeafPage *>(page), mode);
    }
    return IsSafe(reinterpret_cast<InternalPage *>(page), mode);
  }
  //  if (page->IsRootPage()) {
  //    return (mode == ModeType::INSERT && page->GetSize() < page->GetMaxSize() - 1) ||
  //           (mode == ModeType::DELETE && page->GetSize() > 2);
  //  }
  // 如果再插入一个元素，不会产生分裂
  if (mode == ModeType::INSERT) {
    if (page->IsLeafPage()) {
      return page->GetSize() < page->GetMaxSize() - 1;
    }
    return page->GetSize() <= page->GetMaxSize() - 1;
  }
  // 如果再删除一个元素，不会产生并合
  if (mode == ModeType::DELETE) {
    if (page->IsRootPage()) {
      if (page->IsLeafPage()) {
        return page->GetSize() >= leaf_max_size_ / 2 + 1;
      }
      return page->GetSize() >= (internal_max_size_ + 1) / 2 + 1;
    }
    return page->GetSize() >= page->GetMinSize() + 1;
  }
  return true;
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

///***************************************************************************
// *  Check integrity of B+ tree data structure.
// ***************************************************************************/
//
// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::IsBalanced(page_id_t pid) -> int {
//  if (IsEmpty()) {
//    return 1;
//  }
//  auto node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(pid));
//  if (node == nullptr) {
//    throw std::runtime_error("All page are pinned while isBalanced");
//  }
//  int ret = 0;
//  if (!node->IsLeafPage()) {
//    auto page = reinterpret_cast<InternalPage *>(node);
//    int last = -2;
//    for (int i = 0; i < page->GetSize(); i++) {
//      int cur = IsBalanced(page->ValueAt(i));
//      if (cur >= 0 && last == -2) {
//        last = cur;
//        ret = last + 1;
//      } else if (last != cur) {
//        ret = -1;
//        break;
//      }
//    }
//  }
//  buffer_pool_manager_->UnpinPage(pid, false);
//  return ret;
//}
//
// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::IsPageCorr(page_id_t pid, std::pair<KeyType, KeyType> &out) -> bool {
//  if (IsEmpty()) {
//    return true;
//  }
//  auto node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(pid));
//  if (node == nullptr) {
//    throw std::runtime_error("All page are pinned while isPageCorr");
//  }
//  bool ret = true;
//  if (node->IsLeafPage()) {
//    auto page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
//    int size = page->GetSize();
//    ret = ret && (size >= node->GetMinSize() && size <= node->GetMaxSize());
//    for (int i = 1; i < size; i++) {
//      if (comparator_(page->KeyAt(i - 1), page->KeyAt(i)) > 0) {
//        ret = false;
//        break;
//      }
//    }
//    out = std::pair<KeyType, KeyType>{page->KeyAt(0), page->KeyAt(size - 1)};
//  } else {
//    auto page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
//    int size = page->GetSize();
//    ret = ret && (size >= node->GetMinSize() && size <= node->GetMaxSize());
//    std::pair<KeyType, KeyType> left;
//    std::pair<KeyType, KeyType> right;
//    for (int i = 1; i < size; i++) {
//      if (i == 1) {
//        ret = ret && IsPageCorr(page->ValueAt(0), left);
//      }
//      ret = ret && IsPageCorr(page->ValueAt(i), right);
//      ret = ret && (comparator_(page->KeyAt(i), left.second) > 0 && comparator_(page->KeyAt(i), right.first) <= 0);
//      ret = ret && (i == 1 || comparator_(page->KeyAt(i - 1), page->KeyAt(i)) < 0);
//      if (!ret) {
//        break;
//      }
//      left = right;
//    }
//    out = std::pair<KeyType, KeyType>{page->KeyAt(0), page->KeyAt(size - 1)};
//  }
//  buffer_pool_manager_->UnpinPage(pid, false);
//  return ret;
//}
//
// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::Check() -> bool {
//  std::pair<KeyType, KeyType> in;
//  bool is_page_in_order_and_size_corr = IsPageCorr(root_page_id_, in);
//  bool is_bal = (IsBalanced(root_page_id_) >= 0);
//  bool is_all_unpin = buffer_pool_manager_->CheckAllPages();
//  if (!is_page_in_order_and_size_corr) {
//    LOG_DEBUG("Problem in page order or page size");
//  }
//  if (!is_bal) {
//    LOG_DEBUG("Problem in balance");
//  }
//  if (!is_all_unpin) {
//    LOG_DEBUG("Problem in page unpin");
//  }
//  return is_page_in_order_and_size_corr && is_bal && is_all_unpin;
//}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
