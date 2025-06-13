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

#include "sdk/transaction/txn_task/txn_batch_get_task.h"

#include <glog/logging.h>

#include <cstdint>
#include <set>
#include <string>
#include <vector>

#include "common/logging.h"
#include "dingosdk/client.h"
#include "dingosdk/status.h"
#include "sdk/common/common.h"
#include "sdk/common/helper.h"
#include "sdk/transaction/txn_common.h"
#include "sdk/utils/rw_lock.h"
#include "sdk/utils/scoped_cleanup.h"

namespace dingodb {
namespace sdk {

Status TxnBatchGetTask::Init() {
  next_keys_.clear();
  for (const auto& str : keys_) {
    if (!next_keys_.insert(str).second) {
      // duplicate key
      std::string msg = fmt::format("duplicate key: {}", str);
      DINGO_LOG(ERROR) << msg;
      return Status::InvalidArgument(msg);
    }
  }
  return Status::OK();
}

void TxnBatchGetTask::DoAsync() {
  std::set<std::string> next_batch;
  {
    WriteLockGuard guard(rw_lock_);
    next_batch = next_keys_;
    status_ = Status::OK();
  }

  if (next_batch.empty()) {
    DoAsyncDone(Status::OK());
    return;
  }

  std::unordered_map<int64_t, std::shared_ptr<Region>> region_id_to_region;
  std::unordered_map<int64_t, std::vector<std::string>> region_keys;

  auto meta_cache = stub.GetMetaCache();
  for (const auto& key : next_batch) {
    std::shared_ptr<Region> tmp;
    Status s = meta_cache->LookupRegionByKey(key, tmp);
    if (!s.ok()) {
      DoAsyncDone(s);
      return;
    }
    auto iter = region_id_to_region.find(tmp->RegionId());
    if (iter == region_id_to_region.end()) {
      region_id_to_region.emplace(std::make_pair(tmp->RegionId(), tmp));
    }
    region_keys[tmp->RegionId()].push_back(key);
  }

  sub_tasks_count_.store(region_keys.size());
  for (const auto& entry : region_keys) {
    auto region_id = entry.first;
    auto iter = region_id_to_region.find(region_id);
    CHECK(iter != region_id_to_region.end());
    auto region = iter->second;

    TxnBatchGetPartTask* sub_task = new TxnBatchGetPartTask(stub, region_keys[region_id], region, txn_impl_);
    sub_task->AsyncRun([this, sub_task](auto&& s) { SubTaskCallback(std::forward<decltype(s)>(s), sub_task); });
  }
}

void TxnBatchGetTask::SubTaskCallback(const Status& status, TxnBatchGetPartTask* sub_task) {
  SCOPED_CLEANUP({ delete sub_task; });

  if (!status.ok()) {
    DINGO_LOG(WARNING) << "sub_task: " << sub_task->Name() << " fail: " << status.ToString();

    {
      WriteLockGuard guard(rw_lock_);
      if (status_.ok()) {
        // only return first fail status
        status_ = status;
      }
    }

  } else {
    std::vector<KVPair> tmp_out_kvs = sub_task->GetResult();
    {
      WriteLockGuard guard(rw_lock_);
      for (auto& kv : tmp_out_kvs) {
        next_keys_.erase(kv.key);
        if (!kv.value.empty()) {
          out_kvs_.push_back(std::move(kv));
        }
      }
    }
  }

  if (sub_tasks_count_.fetch_sub(1) == 1) {
    Status tmp;
    {
      ReadLockGuard guard(rw_lock_);
      tmp = status_;
    }
    DoAsyncDone(tmp);
  }
}

void TxnBatchGetPartTask ::DoAsync() {
  rpc_.MutableRequest()->Clear();
  rpc_.MutableRequest()->set_start_ts(txn_impl_->GetStartTs());
  FillRpcContext(*rpc_.MutableRequest()->mutable_context(), region_->RegionId(), region_->Epoch(), {resolved_lock_},
                 ToIsolationLevel(txn_impl_->GetOptions().isolation));
  for (const auto& key : keys_) {
    *rpc_.MutableRequest()->add_keys() = key;
  }
  store_rpc_controller_.AsyncCall(
      [this, rpc = &rpc_](Status&& s) { TxnBatchGetPartRpcCallback(std::forward<decltype(s)>(s), rpc); });
}

void TxnBatchGetPartTask::TxnBatchGetPartRpcCallback(const Status& status, TxnBatchGetRpc* rpc) {
  const auto* response = rpc->Response();
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc->Method() << " send to region: " << rpc->Request()->context().region_id()
                       << " fail: " << status.ToString();
    status_ = status;
  } else {
    if (response->has_txn_result()) {
      auto status1 = CheckTxnResultInfo(response->txn_result());
      if (status1.IsTxnLockConflict()) {
        status1 = stub.GetTxnLockResolver()->ResolveLock(response->txn_result().locked(), txn_impl_->GetStartTs());
        if (status1.ok()) {
          // need to retry
          DoAsyncRetry();
          return;
        } else if (status1.IsPushMinCommitTs()) {
          resolved_lock_ = response->txn_result().locked().lock_ts();
          DoAsyncRetry();
          return;
        }
      }
      DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] batch get fail, region({}) status({}) txn_result({}).",
                                        txn_impl_->ID(), rpc->Request()->context().region_id(), status1.ToString(),
                                        response->txn_result().ShortDebugString());
      status_ = status1;
    }
  }

  // some keys may get value even if the status is not ok
  // should we return these successful value?

  if (status_.ok()) {
    for (const auto& kv : response->kvs()) {
      if (!kv.value().empty()) {
        out_kvs_.push_back({kv.key(), kv.value()});
      }
    }
  }

  DoAsyncDone(status_);
}

}  // namespace sdk
}  // namespace dingodb
