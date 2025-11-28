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

#ifndef DINGODB_SDK_BRPC_RW_LOCK_H_
#define DINGODB_SDK_BRPC_RW_LOCK_H_

#include "bthread/bthread.h"
#include "sdk/utils/irw_lock.h"

namespace dingodb {
namespace sdk {
class BthreadRWLock : public IRWLock {
 public:
  BthreadRWLock() { bthread_rwlock_init(&rwlock_, nullptr); }
  ~BthreadRWLock() override { bthread_rwlock_destroy(&rwlock_); }

  void WRLock() override {
    int ret = bthread_rwlock_wrlock(&rwlock_);
    CHECK(0 == ret) << "wlock failed: " << ret << ", " << strerror(ret);
  }

  int TryWRLock() override { return bthread_rwlock_trywrlock(&rwlock_); }

  void UnWRLock() override { bthread_rwlock_unlock(&rwlock_); }

  void RDLock() override {
    int ret = bthread_rwlock_rdlock(&rwlock_);
    CHECK(0 == ret) << "rlock failed: " << ret << ", " << strerror(ret);
  }

  int TryRDLock() override { return bthread_rwlock_tryrdlock(&rwlock_); }

  void UnRDLock() override { bthread_rwlock_unlock(&rwlock_); }

 private:
  bthread_rwlock_t rwlock_;
};

using RWLock = BthreadRWLock;

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_BRPC_RW_LOCK_H_
