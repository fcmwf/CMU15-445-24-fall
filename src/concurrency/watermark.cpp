//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// watermark.cpp
//
// Identification: src/concurrency/watermark.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }
  current_reads_.insert(read_ts);
  if (current_reads_.size() == 1) {
    watermark_ = read_ts;
  } else {
    watermark_ = std::min(watermark_, read_ts);
  }
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
    auto it = current_reads_.find(read_ts);
    if (it != current_reads_.end()) {
      current_reads_.erase(it);
    }

    if (current_reads_.empty()) {
      watermark_ = commit_ts_;
    } else {
      watermark_ = *current_reads_.begin();
    }
}

}  // namespace bustub
