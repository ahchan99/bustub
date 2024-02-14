//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id{};
  if (!free_list_.empty()) {
    // find from the free list
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else if (replacer_->Evict(&frame_id)) {
    // write it back to the disk
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    }
    page_table_->Remove(pages_[frame_id].GetPageId());
  } else {
    return nullptr;
  }
  // call the AllocatePage() method to get a new page id.
  *page_id = AllocatePage();
  // reset the memory and metadata
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  // record the access history of the frame and "Pin" the frame in the replacer
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  page_table_->Insert(*page_id, frame_id);
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id{};
  if (page_table_->Find(page_id, frame_id)) {
    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }
  if (!free_list_.empty()) {
    // find from the free list
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else if (replacer_->Evict(&frame_id)) {
    // write it back to the disk
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    }
    page_table_->Remove(pages_[frame_id].GetPageId());
  } else {
    return nullptr;
  }
  // reset the memory and metadata
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
  // record the access history of the frame in the replacer
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  page_table_->Insert(page_id, frame_id);
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id{};
  if (!page_table_->Find(page_id, frame_id)) {
    LOG_DEBUG("Page error while finding");
    return false;
  }
  if (pages_[frame_id].pin_count_ <= 0) {
    LOG_DEBUG("Page error while unpinning");
    return false;
  }
  pages_[frame_id].pin_count_--;

  // set the dirty flag
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = true;
  }
  if (pages_[frame_id].pin_count_ <= 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id{};
  if (page_id == INVALID_PAGE_ID || !page_table_->Find(page_id, frame_id)) {
    return false;
  }
  // flush a page to disk
  disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
  // unset the dirty flag
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t frame_id = 0; frame_id < pool_size_; frame_id++) {
    FlushPgImp(pages_[frame_id].GetPageId());
  }
}

// auto BufferPoolManagerInstance::CheckAllPgsImp() -> bool {
//   bool res = true;
//   for (size_t i = 1; i < pool_size_; i++) {
//     if (pages_[i].pin_count_ != 0 && pages_[i].GetPageId() != 0) { // 过滤 header page
//       res = false;
//       std::string const log =
//           "Page: " + std::to_string(pages_[i].GetPageId()) + " pin count: " + std::to_string(pages_[i].pin_count_);
//       LOG_DEBUG("%s", log.data());
//     }
//   }
//   return res;
// }
auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id{};
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  if (pages_[frame_id].pin_count_ > 0) {
    LOG_DEBUG("Page pin count > 0 while deleting");
    return false;
  }
  replacer_->Remove(frame_id);
  page_table_->Remove(page_id);
  free_list_.emplace_back(frame_id);
  // reset the memory and metadata
  pages_[frame_id].ResetMemory();
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].is_dirty_ = false;
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
