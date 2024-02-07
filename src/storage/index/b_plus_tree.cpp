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
  if (IsEmpty()) {
    return false;
  }
  auto left_page = GetLeafPage(key);
  auto ret = left_page->GetValue(key, result, comparator_);
  buffer_pool_manager_->UnpinPage(left_page->GetPageId(), false);
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
  // New a leaf page as root of b+tree and store the key and value
  if (IsEmpty()) {
    auto root_page = NewPage<LeafPage>();
    root_page_id_ = root_page->GetPageId();
    // Call this method everytime root page id is changed.
    UpdateRootPageId(0);
    // Insert into new leaf page
    auto ret = root_page->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(root_page->GetPageId(), true);
    return ret;
  }
  // Insert into leaf page
  auto leaf_page = GetLeafPage(key);
  auto ret = leaf_page->Insert(key, value, comparator_);
  // Duplicate key
  if (!ret) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return false;
  }
  if (leaf_page->GetSize() < leaf_page->GetMaxSize()) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return true;
  }
  // Splitting condition: number of key/value pairs AFTER insertion equals to max_size for leaf nodes
  auto new_leaf_page = Split(leaf_page);
  auto risen_key = new_leaf_page->KeyAt(0);
  ret = InsertIntoParent(leaf_page, new_leaf_page, risen_key, transaction);
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
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

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
  auto leftmost_leaf_page = GetLeftmostLeafPage();
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leftmost_leaf_page, 0);
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
  auto leaf_page = GetLeafPage(key);
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
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  auto rightmost_leaf_page = GetRightmostLeafPage();
  return INDEXITERATOR_TYPE(buffer_pool_manager_, rightmost_leaf_page, rightmost_leaf_page->GetSize());
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

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
    buffer_pool_manager_->UnpinPage(page_id, true);
    return reinterpret_cast<T *>(new_page);
  }
  auto new_page = reinterpret_cast<InternalPage *>(page->GetData());
  new_page->Init(page_id, INVALID_PAGE_ID, internal_max_size_);
  buffer_pool_manager_->UnpinPage(page_id, true);
  return reinterpret_cast<T *>(new_page);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename T>
auto BPLUSTREE_TYPE::Split(T *page) -> T * {
  static_assert(std::is_same_v<T, LeafPage> || std::is_same_v<T, InternalPage>);
  auto new_page = NewPage<T>();
  if (std::is_same_v<T, LeafPage>) {
    auto leaf_page = reinterpret_cast<LeafPage *>(page);
    auto new_leaf_page = reinterpret_cast<LeafPage *>(new_page);
    new_leaf_page->SetParentPageId(leaf_page->GetParentPageId());
    leaf_page->MoveHalfTo(new_leaf_page);
    new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
    leaf_page->SetNextPageId(new_leaf_page->GetPageId());
    return reinterpret_cast<T *>(new_leaf_page);
  }
  auto internal_page = reinterpret_cast<InternalPage *>(page);
  auto new_internal_page = reinterpret_cast<InternalPage *>(new_page);
  new_internal_page->SetParentPageId(internal_page->GetParentPageId());
  internal_page->MoveHalfTo(new_internal_page, buffer_pool_manager_);
  return reinterpret_cast<T *>(new_internal_page);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPage(const KeyType &key) -> BPlusTree::LeafPage * {
  assert(!IsEmpty());
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto internal_page = reinterpret_cast<InternalPage *>(page->GetData());
  auto page_id = root_page_id_;
  while (!internal_page->IsLeafPage()) {
    auto child_page_id = internal_page->GetValue(key, comparator_);
    page = buffer_pool_manager_->FetchPage(child_page_id);
    internal_page = reinterpret_cast<InternalPage *>(page->GetData());
    buffer_pool_manager_->UnpinPage(page_id, false);
    page_id = child_page_id;
  }
  // now internal_page is a leaf node
  auto leaf_page = reinterpret_cast<LeafPage *>(internal_page);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return leaf_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeftmostLeafPage() -> BPlusTree::LeafPage * {
  assert(!IsEmpty());
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto internal_page = reinterpret_cast<InternalPage *>(page->GetData());
  auto page_id = root_page_id_;
  while (!internal_page->IsLeafPage()) {
    auto child_page_id = internal_page->ValueAt(0);
    page = buffer_pool_manager_->FetchPage(child_page_id);
    internal_page = reinterpret_cast<InternalPage *>(page->GetData());
    buffer_pool_manager_->UnpinPage(page_id, false);
    page_id = child_page_id;
  }
  // now internal_page is a leaf node
  auto leaf_page = reinterpret_cast<LeafPage *>(internal_page);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return leaf_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRightmostLeafPage() -> BPlusTree::LeafPage * {
  assert(!IsEmpty());
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto internal_page = reinterpret_cast<InternalPage *>(page->GetData());
  auto page_id = root_page_id_;
  while (!internal_page->IsLeafPage()) {
    auto child_page_id = internal_page->ValueAt(internal_page->GetSize() - 1);
    page = buffer_pool_manager_->FetchPage(child_page_id);
    internal_page = reinterpret_cast<InternalPage *>(page->GetData());
    buffer_pool_manager_->UnpinPage(page_id, false);
    page_id = child_page_id;
  }
  // now internal_page is a leaf node
  auto leaf_page = reinterpret_cast<LeafPage *>(internal_page);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return leaf_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_page, BPlusTreePage *new_page, const KeyType &risen_key,
                                      Transaction *transaction) -> bool {
  if (old_page->IsRootPage()) {
    auto new_root_page = NewPage<InternalPage>();
    new_root_page->SetValueAt(0, old_page->GetPageId());
    new_root_page->SetKeyAt(1, risen_key);
    new_root_page->SetValueAt(1, new_page->GetPageId());
    new_root_page->SetSize(2);
    root_page_id_ = new_root_page->GetPageId();
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    old_page->SetParentPageId(root_page_id_);
    new_page->SetParentPageId(root_page_id_);
    return true;
  }
  auto page = buffer_pool_manager_->FetchPage(old_page->GetParentPageId());
  auto *parent_page = reinterpret_cast<InternalPage *>(page->GetData());
  auto ret = parent_page->Insert(old_page->GetPageId(), risen_key, new_page->GetPageId());
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  if (parent_page->GetSize() <= internal_max_size_) {
    return ret;
  }
  // Splitting condition: number of children BEFORE insertion equals to max_size for internal nodes
  auto new_parent_page = Split(parent_page);
  // parent_risen_key 被挪到父节点，等价删除
  auto parent_risen_key = new_parent_page->KeyAt(0);
  ret = InsertIntoParent(parent_page, new_parent_page, parent_risen_key, transaction);
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_parent_page->GetPageId(), true);
  return ret;
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

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
