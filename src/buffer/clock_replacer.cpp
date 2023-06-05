//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <iterator>
#include "common/config.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
  this->num_pages_ = num_pages;
  this->pointer_ = 0;
  this->ref_flag_ = new bool[num_pages];
  memset(this->ref_flag_, 0, num_pages);
}

ClockReplacer::~ClockReplacer() { delete[] this->ref_flag_; }

auto ClockReplacer::Victim(frame_id_t *frame_id) -> bool {
  latch_.lock();
  frame_id_t current = this->pointer_;
  for (size_t i = 0; i < num_pages_; i++) {
    if (!this->ref_flag_[current]) {
      current = NextSlot(current);
      continue;
    }
    *frame_id = current;
    this->ref_flag_[current] = false;
    this->pointer_ = NextSlot(current);
    latch_.unlock();
    return true;
  }
  latch_.unlock();
  return false;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  latch_.lock();
  this->pointer_ = NextSlot(frame_id);
  this->ref_flag_[frame_id] = false;
  latch_.unlock();
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  latch_.lock();
  this->ref_flag_[frame_id] = true;
  latch_.unlock();
}

auto ClockReplacer::Size() -> size_t {
  latch_.lock();
  size_t size = 0;
  for (size_t i = 0; i < this->num_pages_; i++) {
    if (this->ref_flag_[i]) {
      size++;
    }
  }
  latch_.unlock();
  return size;
}

auto ClockReplacer::NextSlot(frame_id_t slot) -> frame_id_t { return (slot + 1) % this->num_pages_; }

}  // namespace bustub
