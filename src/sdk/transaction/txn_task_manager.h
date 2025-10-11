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

#ifndef DINGODB_SDK_TRANSACTION_TXN_TASK_MANAGER_H_
#define DINGODB_SDK_TRANSACTION_TXN_TASK_MANAGER_H_

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <unordered_set>

#include "fmt/core.h"

namespace dingodb {
namespace sdk {

class TxnTaskManager {
 public:
  TxnTaskManager() = default;
  ~TxnTaskManager();

  std::string GenerateTaskName(int64_t txn_id, const std::string& task_type);

  void RegisterTask(const std::string& task_name);

  void UnregisterTask(const std::string& task_name);

  void WaitAllTasksComplete();

  void Stop();

  bool IsStopped() const { return stopped_.load(); }

  size_t GetActiveTaskCount() const;

 private:
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::unordered_set<std::string> active_tasks_;
  std::atomic<bool> stopped_{false};
  std::atomic<int64_t> task_counter_{0};
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_TRANSACTION_TXN_TASK_MANAGER_H_