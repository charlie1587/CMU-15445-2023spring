//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  Page *ans = nullptr;
  // if free list or the replacer has place
  if (!free_list_.empty() || replacer_->Size() > 0) {
    frame_id_t frame_id = 0;
    if (!free_list_.empty()) {
      // allocate a new frame
      frame_id = free_list_.front();
      free_list_.pop_front();
    } else {
      // allocate a new frame
      replacer_->Evict(&frame_id);
      // check if write the dirty page to the disk
      if (pages_[frame_id].IsDirty()) {
        disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
      }
      // erase the page table & reset
      page_table_.erase(pages_[frame_id].page_id_);
      pages_[frame_id].ResetMemory();
      DeallocatePage(pages_[frame_id].GetPageId());
    }
    // allocate new page id
    page_id_t page_new_id = AllocatePage();
    *page_id = page_new_id;
    // reset the page
    pages_[frame_id].page_id_ = *page_id;
    pages_[frame_id].pin_count_ = 1;
    pages_[frame_id].is_dirty_ = false;
    // Add page table
    page_table_[*page_id] = frame_id;
    // Add the page into the replacer and pin the page
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    // Get the Page ptr
    ans = &pages_[frame_id];
  }
  return ans;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  Page *ans = nullptr;
  // find in the replacer
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    ans = &pages_[frame_id];
    // not find in the replacer
  } else {
    if (!free_list_.empty() || replacer_->Size() > 0) {
      frame_id_t frame_id = 0;
      if (!free_list_.empty()) {
        // allocate a new frame
        frame_id = free_list_.front();
        free_list_.pop_front();
      } else {
        // allocate a new frame
        replacer_->Evict(&frame_id);
        // check if write the dirty page to the disk
        if (pages_[frame_id].IsDirty()) {
          disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
        }
        // erase the page table
        page_table_.erase(pages_[frame_id].page_id_);
        pages_[frame_id].ResetMemory();
        DeallocatePage(pages_[frame_id].GetPageId());
      }
      // reset the page
      disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
      pages_[frame_id].page_id_ = page_id;
      pages_[frame_id].pin_count_ = 1;
      pages_[frame_id].is_dirty_ = false;
      // Add page table
      page_table_[page_id] = frame_id;
      // Add the page into the replacer and pin the page
      replacer_->RecordAccess(frame_id);
      replacer_->SetEvictable(frame_id, false);
      // Get the Page ptr
      ans = &pages_[frame_id];
    }
  }
  return ans;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.find(page_id) == page_table_.end() || pages_[page_table_[page_id]].pin_count_ <= 0) {
    return false;
  }
  pages_[page_table_[page_id]].pin_count_--;
  pages_[page_table_[page_id]].is_dirty_ = (is_dirty || pages_[page_table_[page_id]].is_dirty_);
  if (pages_[page_table_[page_id]].pin_count_ == 0) {
    replacer_->SetEvictable(page_table_[page_id], true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  if (pages_[page_table_[page_id]].pin_count_ > 0) {
    return false;
  }
  replacer_->Remove(page_table_[page_id]);
  free_list_.emplace_back(page_table_[page_id]);
  // reset the page
  pages_[page_table_[page_id]].ResetMemory();
  pages_[page_table_[page_id]].page_id_ = INVALID_PAGE_ID;
  pages_[page_table_[page_id]].is_dirty_ = false;
  pages_[page_table_[page_id]].pin_count_ = 0;
  page_table_.erase(page_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub
