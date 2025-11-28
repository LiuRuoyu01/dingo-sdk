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

#ifndef DINGODB_SDK_GRPC_RW_LOCK_H_
#define DINGODB_SDK_GRPC_RW_LOCK_H_

#include <shared_mutex>

#include "sdk/utils/irw_lock.h"

namespace dingodb {
namespace sdk {

class PthreadRWLock : public IRWLock {
 public:
  PthreadRWLock() = default;
  ~PthreadRWLock() override = default;

  void WRLock() override { mutex_.lock(); }

  int TryWRLock() override { return mutex_.try_lock() ? 0 : 1; }

  void UnWRLock() override { mutex_.unlock(); }

  void RDLock() override { mutex_.lock_shared(); }

  int TryRDLock() override { return mutex_.try_lock_shared() ? 0 : 1; }

  void UnRDLock() override { mutex_.unlock_shared(); }

 private:
  std::shared_mutex mutex_;
};

using RWLock = PthreadRWLock;
}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_GRPC_RW_LOCK_H_