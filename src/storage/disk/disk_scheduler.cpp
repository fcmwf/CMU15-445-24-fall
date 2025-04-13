//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // Spawn the background thread
  for (size_t i = 0; i < pool_size_; i++) {
    background_threads_.emplace_back([this] { WorkerThread(); });
  }
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);

  for (size_t i = 0; i < pool_size_; i++) {
    background_threads_[i].join();
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Schedules a request for the DiskManager to execute.
 *
 * @param r The request to be scheduled.
 */
void DiskScheduler::Schedule(DiskRequest r) { request_queue_.Put(std::move(r)); }

/**
 * TODO(P1): Add implementation
 *
 * @brief Background worker thread function that processes scheduled requests.
 *
 * The background thread needs to process requests while the DiskScheduler exists, i.e., this function should not
 * return until ~DiskScheduler() is called. At that point you need to make sure that the function does return.
 */
void DiskScheduler::WorkerThread() {
  while (1) {
    auto req = request_queue_.Get();
    if (!req.has_value()) {
      request_queue_.Put(std::nullopt);
      break;
    } else {
      DiskRequest req_value = std::move(req.value());
      if (req_value.is_write_) {
        disk_manager_->WritePage(req_value.page_id_, req_value.data_);
      } else {
        disk_manager_->ReadPage(req_value.page_id_, req_value.data_);
      }
      req_value.callback_.set_value(true);
    }
  }
}
}  // namespace bustub
