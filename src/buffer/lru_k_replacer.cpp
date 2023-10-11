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
#include <algorithm>
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if (node_store_[*it].is_evictable_) {
      *frame_id = *it;
      list_.erase(it);
      node_store_.erase(*frame_id);
      curr_size_--;
      latch_.unlock();
      return true;
    }
  }
  latch_.unlock();
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  latch_.lock();
  if (node_store_.find(frame_id) != node_store_.end()) {
    node_store_[frame_id].history_.push_back(current_timestamp_++);
    for (auto it = list_.begin(); it != list_.end(); it++) {
      if (*it == frame_id) {
        list_.erase(it);
        break;
      }
    }
  } else {
    if (curr_size_ >= replacer_size_) {
      latch_.unlock();
      throw Exception("Replacer is full!");
    }
    LRUKNode tmp_node;
    tmp_node.fid_ = frame_id;
    tmp_node.history_.push_back(current_timestamp_++);
    tmp_node.is_evictable_ = true;
    curr_size_++;
    node_store_[frame_id] = tmp_node;
  }
  // count this frame back-k-distance
  size_t frame_k_size = node_store_[frame_id].history_.size();
  size_t frame_k =
      (frame_k_size >= k_) ? current_timestamp_ - node_store_[frame_id].history_[frame_k_size - k_] : INT_FAST32_MAX;
  // insert the frame_id
  for (auto it = list_.begin(); it != list_.end(); it++) {
    size_t frame_it_size = node_store_[*it].history_.size();
    size_t frame_it =
        (frame_it_size >= k_) ? current_timestamp_ - node_store_[*it].history_[frame_it_size - k_] : INT_FAST32_MAX;
    bool finish_insert = false;
    if (frame_k > frame_it) {
      list_.insert(it, frame_id);
      latch_.unlock();
      finish_insert = true;
    } else if (frame_k == frame_it && frame_k == INT_FAST32_MAX) {
      if (node_store_[frame_id].history_[0] < node_store_[*it].history_[0]) {
        list_.insert(it, frame_id);
        latch_.unlock();
        finish_insert = true;
      }
    }
    if (finish_insert) {
      return;
    }
  }
  list_.push_back(frame_id);
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (node_store_.find(frame_id) == node_store_.end()) {
    throw Exception("Invalid frame id!");
  }
  latch_.lock();
  if (node_store_[frame_id].is_evictable_ && !set_evictable) {
    node_store_[frame_id].is_evictable_ = set_evictable;
    curr_size_--;
  } else if (!node_store_[frame_id].is_evictable_ && set_evictable) {
    node_store_[frame_id].is_evictable_ = set_evictable;
    curr_size_++;
  }
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  //  current_timestamp_ = time(NULL);
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if (frame_id == *it) {
      // check if is evicted able
      if (!node_store_[frame_id].is_evictable_) {
        throw Exception("Not evictable!");
      }
      // lock
      list_.erase(it);
      node_store_.erase(frame_id);
      curr_size_--;
      latch_.unlock();
      return;
    }
  }
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t {
  latch_.lock();
  size_t cur_size = curr_size_;
  latch_.unlock();
  return cur_size;
}

}  // namespace bustub
