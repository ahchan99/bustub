//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (curr_size_ == 0) {
    return false;
  }
  for (auto iter = inf_history_list_.begin(); iter != inf_history_list_.end(); iter++) {
    if (non_evictable_set_.count(*iter) == 0) {
      *frame_id = *iter;
      count_map_.erase(*iter);
      inf_history_list_.erase(iter);
      curr_size_--;
      return true;
    }
  }
  for (auto iter = history_list_.begin(); iter != history_list_.end(); iter++) {
    if (non_evictable_set_.count(*iter) == 0) {
      *frame_id = *iter;
      count_map_.erase(*iter);
      history_list_.erase(iter);
      curr_size_--;
      return true;
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  // first record
  if (count_map_.count(frame_id) == 0) {
    if (curr_size_ == replacer_size_) {
      BUSTUB_ASSERT(frame_id, "frame id is invalid");
    }
    inf_history_list_.push_back(frame_id);
    count_map_[frame_id] = 1;
    curr_size_++;
    return;
  }
  // record in not +inf list (LRU)
  if (count_map_[frame_id] >= k_) {
    // find it push back
    for (auto iter = history_list_.begin(); iter != history_list_.end(); iter++) {
      if (*iter == frame_id) {
        history_list_.erase(iter);
        count_map_[frame_id]++;
        history_list_.push_back(frame_id);
        return;
      }
    }
  }
  // record in +inf list (FIFO)
  count_map_[frame_id]++;
  // change to not +inf list
  if (count_map_[frame_id] >= k_) {
    // find it push back
    for (auto iter = inf_history_list_.begin(); iter != inf_history_list_.end(); iter++) {
      if (*iter == frame_id) {
        inf_history_list_.erase(iter);
        history_list_.push_back(frame_id);
        return;
      }
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (count_map_.count(frame_id) == 0) {
    BUSTUB_ASSERT(frame_id, "frame id is invalid");
  }
  if (non_evictable_set_.count(frame_id) == 0 && !set_evictable) {
    non_evictable_set_.insert(frame_id);
    curr_size_--;
  }
  if (non_evictable_set_.count(frame_id) != 0 && set_evictable) {
    non_evictable_set_.erase(frame_id);
    curr_size_++;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (count_map_.count(frame_id) == 0) {
    return;
  }
  if (non_evictable_set_.count(frame_id) != 0) {
    throw "LRUKReplacer::Remove: frame_id is non-evictable";
  }
  if (count_map_[frame_id] >= k_) {
    for (auto iter = history_list_.begin(); iter != history_list_.end(); iter++) {
      if (*iter == frame_id) {
        history_list_.erase(iter);
        curr_size_--;
        return;
      }
    }
  }

  for (auto iter = inf_history_list_.begin(); iter != inf_history_list_.end(); iter++) {
    if (*iter == frame_id) {
      inf_history_list_.erase(iter);
      curr_size_--;
      return;
    }
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
