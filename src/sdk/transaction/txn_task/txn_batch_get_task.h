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

#ifndef TXN_BATCH_GET_TASK_H
#define TXN_BATCH_GET_TASK_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "dingosdk/client.h"
#include "sdk/client_stub.h"
#include "sdk/region.h"
#include "sdk/rpc/store_rpc_controller.h"
#include "sdk/transaction/txn_impl.h"
#include "sdk/transaction/txn_task/txn_task.h"
#include "sdk/utils/rw_lock.h"

namespace dingodb {
namespace sdk {

class TxnBatchGetPartTask;
class TxnBatchGetTask : public TxnTask {
 public:
  TxnBatchGetTask(const ClientStub& stub, const std::vector<std::string>& key, std::vector<KVPair>& out_kvs,
                  std::shared_ptr<Transaction::TxnImpl> txn_impl)
      : TxnTask(stub), keys_(key), out_kvs_(out_kvs), txn_impl_(txn_impl) {}

  ~TxnBatchGetTask() override = default;

 private:
  Status Init() override;

  void DoAsync() override;

  std::string Name() const override { return "TxnBatchGetTask"; }

  void SubTaskCallback(const Status& status, TxnBatchGetPartTask* sub_task);

  const std::vector<std::string>& keys_;
  std::vector<KVPair>& out_kvs_;

  std::set<std::string> next_keys_;
  std::shared_ptr<Transaction::TxnImpl> txn_impl_;

  RWLock rw_lock_;
  Status status_;

  std::atomic<int> sub_tasks_count_{0};
};

class TxnBatchGetPartTask : public TxnTask {
 public:
  TxnBatchGetPartTask(const ClientStub& stub, const std::vector<std::string>& keys, std::shared_ptr<Region> region,
                      std::shared_ptr<Transaction::TxnImpl> txn_impl)
      : TxnTask(stub), keys_(keys), region_(region), txn_impl_(txn_impl), store_rpc_controller_(stub, rpc_,region) {}

  ~TxnBatchGetPartTask() override = default;

  std::vector<KVPair> GetResult() { return out_kvs_; }

 private:
  friend class TxnBatchGetTask;

  void DoAsync() override;

  std::string Name() const override { return "TxnBatchGetPartTask"; }

  void TxnBatchGetPartRpcCallback(const Status& status, TxnBatchGetRpc* rpc);

  const std::vector<std::string>& keys_;
  std::shared_ptr<Region> region_;
  std::vector<KVPair> out_kvs_;
  std::shared_ptr<Transaction::TxnImpl> txn_impl_;

  Status status_;
  uint64_t resolved_lock_{0};
  StoreRpcController store_rpc_controller_;
  TxnBatchGetRpc rpc_;
};

}  // namespace sdk

}  // namespace dingodb

#endif  // DINGODB_SDK_TRANSACTION_BUFFER_H_