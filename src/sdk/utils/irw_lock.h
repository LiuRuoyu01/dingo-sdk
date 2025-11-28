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

#ifndef DINGODB_SDK_IRW_LOCK_H_
#define DINGODB_SDK_IRW_LOCK_H_

namespace dingodb {
namespace sdk {

class IRWLock {
 public:
  IRWLock(const IRWLock&) = delete;
  IRWLock& operator=(const IRWLock&) = delete;

  virtual void WRLock() = 0;
  virtual int TryWRLock() = 0;
  virtual void UnWRLock() = 0;

  virtual void RDLock() = 0;
  virtual int TryRDLock() = 0;
  virtual void UnRDLock() = 0;

 protected:
  IRWLock() = default;
  virtual ~IRWLock() = default;
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_IRW_LOCK_H_
