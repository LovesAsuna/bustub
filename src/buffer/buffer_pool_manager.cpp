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
#include <cassert>
#include <cstddef>
#include <mutex>

#include "buffer/clock_replacer.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "libfort/lib/fort.hpp"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<ClockReplacer>(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::UpdatePage(Page *page, page_id_t new_page_id, frame_id_t new_frame_id) {
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->page_id_, page->data_);
    page->is_dirty_ = false;
  }

  page_table_.erase(page->page_id_);
  if (new_page_id != INVALID_PAGE_ID) {
    page_table_.emplace(new_page_id, new_frame_id);
  }

  page->ResetMemory();
  page->page_id_ = new_page_id;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  frame_id_t frame_id = -1;
  if (!FindVictimPage(&frame_id)) {
    return nullptr;
  }
  *page_id = AllocatePage();
  Page *page = &pages_[frame_id];
  UpdatePage(page, *page_id, frame_id);
  replacer_->Pin(frame_id);
  page->pin_count_ = 1;
  assert(page->pin_count_ == 1);
  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {
    frame_id_t frame_id = iter->second;
    Page *page = &pages_[frame_id];
    replacer_->Pin(frame_id);
    page->pin_count_++;
    return page;
  }

  frame_id_t frame_id = -1;
  if (!FindVictimPage(&frame_id)) {
    return nullptr;
  }
  Page *page = &pages_[frame_id];
  UpdatePage(page, page_id, frame_id);
  disk_manager_->ReadPage(page_id, page->data_);
  replacer_->Pin(frame_id);
  page->pin_count_++;
  assert(page->pin_count_ == 1);
  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = iter->second;
  Page *page = &pages_[frame_id];

  if (page->pin_count_ == 0) {
    return false;
  }

  page->pin_count_--;
  if (page->pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }

  if (is_dirty) {
    page->is_dirty_ = true;
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = iter->second;
  Page *page = &pages_[frame_id];

  disk_manager_->WritePage(page->page_id_, page->data_);
  page->is_dirty_ = false;

  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (size_t i = 0; i < pool_size_; i++) {
    Page *page = &pages_[i];
    if (page->page_id_ != INVALID_PAGE_ID) {
      disk_manager_->WritePage(page->page_id_, page->data_);
      page->is_dirty_ = false;
    }
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = iter->second;
  Page *page = &pages_[frame_id];

  if (page->pin_count_ > 0) {
    return false;
  }

  UpdatePage(page, INVALID_PAGE_ID, frame_id);
  free_list_.push_back(frame_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FindVictimPage(frame_id_t *frame_id) -> bool {
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    return true;
  }

  return replacer_->Victim(frame_id);
}

}  // namespace bustub
