#include "storage/page/page_guard.h"
#include <algorithm>
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  Drop();
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;

  that.bpm_ = nullptr;
  that.bpm_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (bpm_ == nullptr || page_ == nullptr) {
    return;
  }
  bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  bpm_ = nullptr;
  page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  Drop();
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { guard_ = std::move(that.guard_); }

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  guard_.Drop();
  guard_.bpm_ = that.guard_.bpm_;
  guard_.page_ = that.guard_.page_;
  guard_.is_dirty_ = that.guard_.is_dirty_;
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.bpm_ == nullptr || guard_.page_ == nullptr) {
    return;
  }
  RUnlatch();
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { guard_ = std::move(that.guard_); }

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  guard_.Drop();
  guard_.bpm_ = that.guard_.bpm_;
  guard_.page_ = that.guard_.page_;
  guard_.is_dirty_ = that.guard_.is_dirty_;
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.bpm_ == nullptr || guard_.page_ == nullptr) {
    return;
  }
  RUnlatch();
  WUnlatch();
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
