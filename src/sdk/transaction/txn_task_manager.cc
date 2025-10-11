// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "sdk/transaction/txn_task_manager.h"

#include "common/logging.h"

namespace dingodb {
namespace sdk {

TxnTaskManager::~TxnTaskManager() {
  DINGO_LOG(INFO) << "TxnTaskManager destructor start";
  Stop();
  WaitAllTasksComplete();
  DINGO_LOG(INFO) << "TxnTaskManager destructor end";
}

std::string TxnTaskManager::GenerateTaskName(int64_t txn_id, const std::string& task_type) {
  int64_t counter = task_counter_.fetch_add(1);
  return fmt::format("{}_{}__{}", txn_id, task_type, counter);
}

void TxnTaskManager::RegisterTask(const std::string& task_name) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (stopped_.load()) {
    DINGO_LOG(WARNING) << fmt::format("TxnTaskManager is stopped, ignore task: {}", task_name);
    return;
  }

  active_tasks_.insert(task_name);
  DINGO_LOG(DEBUG) << fmt::format("Register task: {}, active tasks: {}", task_name, active_tasks_.size());
}

void TxnTaskManager::UnregisterTask(const std::string& task_name) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = active_tasks_.find(task_name);
  if (it != active_tasks_.end()) {
    active_tasks_.erase(it);
    DINGO_LOG(DEBUG) << fmt::format("Unregister task: {}, active tasks: {}", task_name, active_tasks_.size());
  } else {
    DINGO_LOG(WARNING) << fmt::format("Task not found for unregister: {}", task_name);
  }

  if (active_tasks_.empty()) {
    cv_.notify_all();
  }
}

void TxnTaskManager::WaitAllTasksComplete() {
  std::unique_lock<std::mutex> lock(mutex_);

  if (active_tasks_.empty()) {
    DINGO_LOG(DEBUG) << "No active tasks, return immediately";
    return;
  }

  DINGO_LOG(INFO) << fmt::format("Waiting for {} tasks to complete", active_tasks_.size());

  // wait until all tasks complete
  cv_.wait(lock, [this] { return active_tasks_.empty(); });
}

void TxnTaskManager::Stop() {
  bool expected = false;
  if (stopped_.compare_exchange_strong(expected, true)) {
    DINGO_LOG(INFO) << "TxnTaskManager stopped, no more tasks will be accepted";
  }
  WaitAllTasksComplete();
}

size_t TxnTaskManager::GetActiveTaskCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return active_tasks_.size();
}

}  // namespace sdk
}  // namespace dingodb