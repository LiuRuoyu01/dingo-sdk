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

#include "sdk/transaction/txn_task/txn_prewrite_task.h"

#include <cstdint>

#include "dingosdk/status.h"
#include "sdk/common/common.h"
#include "sdk/common/helper.h"
#include "sdk/region.h"
#include "sdk/rpc/store_rpc_controller.h"
#include "sdk/transaction/txn_buffer.h"
#include "sdk/transaction/txn_common.h"
#include "sdk/transaction/txn_lock_resolver.h"
#include "sdk/utils/callback.h"

namespace dingodb {
namespace sdk {

Status TxnPrewriteTask::Init() {
  next_mutations_.clear();
  for (const auto& [key, mutation] : mutations_) {
    if (next_mutations_.find(mutation->key) != next_mutations_.end()) {
      // duplicate mutation
      std::string msg = fmt::format("duplicate mutation: {}", mutation->ToString());
      DINGO_LOG(ERROR) << msg;
      return Status::InvalidArgument(msg);
    } else {
      next_mutations_.emplace(mutation->key, mutation);
    }
  }
  return Status::OK();
}

void TxnPrewriteTask::DoAsync() {
  std::map<std::string, const TxnMutation*> next_batch;
  {
    WriteLockGuard guard(rw_lock_);
    next_batch = next_mutations_;
    status_ = Status::OK();
  }

  if (next_batch.empty()) {
    DoAsyncDone(Status::OK());
    return;
  }

  std::unordered_map<int64_t, std::shared_ptr<Region>> region_id_to_region;
  std::unordered_map<int64_t, std::vector<const TxnMutation*>> region_mutations;

  auto meta_cache = stub.GetMetaCache();
  for (const auto& [_, mutation] : next_batch) {
    std::shared_ptr<Region> tmp;
    Status s = meta_cache->LookupRegionByKey(mutation->key, tmp);
    if (!s.ok()) {
      DoAsyncDone(s);
      return;
    }
    auto iter = region_id_to_region.find(tmp->RegionId());
    if (iter == region_id_to_region.end()) {
      region_id_to_region.emplace(std::make_pair(tmp->RegionId(), tmp));
    }

    if (region_mutations.find(tmp->RegionId()) == region_mutations.end()) {
      region_mutations[tmp->RegionId()] = {mutation};
    } else {
      if (primary_key_ != mutation->key) {
        region_mutations[tmp->RegionId()].push_back(mutation);
      } else {
        // If primary key is in the mutations, we need to put it at the front of the mutations list
        const auto* front = region_mutations[tmp->RegionId()].front();
        region_mutations[tmp->RegionId()][0] = mutation;
        region_mutations[tmp->RegionId()].push_back(front);
      }
    }
  }

  // only first run need to check if it is one pc , maybe some error casue only one region retry at the second run
  if (first_run_ && region_mutations.size() == 1) {
    is_one_pc_ = true;
  }

  sub_tasks_count_.store(region_mutations.size());

  for (const auto& entry : region_mutations) {
    auto region_id = entry.first;
    auto iter = region_id_to_region.find(region_id);
    CHECK(iter != region_id_to_region.end());
    auto region = iter->second;

    auto rpc = std::make_unique<TxnPrewriteRpc>();
    rpc->MutableRequest()->Clear();
    rpc->MutableRequest()->set_start_ts(txn_impl_->GetStartTs());
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->Epoch(),
                   ToIsolationLevel(txn_impl_->GetOptions().isolation));
    rpc->MutableRequest()->set_primary_lock(primary_key_);
    rpc->MutableRequest()->set_lock_ttl(INT64_MAX);
    rpc->MutableRequest()->set_txn_size(mutations_.size());
    rpc->MutableRequest()->set_try_one_pc(is_one_pc_);

    for (const auto& mutation : entry.second) {
      TxnMutation2MutationPB(*mutation, rpc->MutableRequest()->add_mutations());
    }

    StoreRpcController controller(stub, *rpc, region);
    controllers_.push_back(std::move(controller));
    rpcs_.push_back(std::move(rpc));
  }

  CHECK(rpcs_.size() == region_mutations.size());
  CHECK(controllers_.size() == region_mutations.size());

  sub_tasks_count_.store(region_mutations.size());

  for (int i = 0; i < rpcs_.size(); ++i) {
    auto& controller = controllers_[i];
    controller.AsyncCall([this, rpc = rpcs_[i].get()](const Status& s) { TxnPrewriteRpcCallback(s, rpc); });
  }
}

void TxnPrewriteTask::TxnPrewriteRpcCallback(const Status& status, TxnPrewriteRpc* rpc) {
  Status s;
  const auto* response = rpc->Response();
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc->Method() << " send to region: " << rpc->Request()->context().region_id()
                       << " fail: " << status.ToString();

    s = status;
  } else {
    Status s1;
    for (const auto& txn_result : response->txn_result()) {
      s1 = CheckTxnResultInfo(txn_result);
      if (s1.ok()) {
        continue;
      } else if (s1.IsTxnLockConflict()) {
        s1 = stub.GetTxnLockResolver()->ResolveLock(txn_result.locked(), txn_impl_->GetStartTs());
        if (!s1.ok()) {
          DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] precommit resolve lock fail, pk() status({}) txn_result({}).",
                                            txn_impl_->ID(), StringToHex(primary_key_), s.ToString(),
                                            txn_result.ShortDebugString());
          if (s.ok()) {
            s = s1;  // only return first fail status
          }
        }

      } else if (s1.IsTxnWriteConflict()) {
        DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] precommit write conflict, pk({}) status({}) txn_result({}).",
                                          txn_impl_->ID(), StringToHex(primary_key_), s.ToString(),
                                          txn_result.ShortDebugString());
        if (s.ok()) {
          s = s1;  // only return first fail status
        }
        break;

      } else {
        DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] precommit unexpect response, pk({}) , status({}) response({}).",
                                          txn_impl_->ID(), StringToHex(primary_key_), s.ToString(),
                                          response->ShortDebugString());
        if (s.ok()) {
          s = s1;  // only return first fail status
        }
        break;
      }
    }
  }

  {
    WriteLockGuard guard(rw_lock_);
    if (status_.ok()) {
      if (s.ok()) {
        for (const auto& mutation : rpc->Request()->mutations()) {
          next_mutations_.erase(mutation.key());
        }
      } else {
        // only return first fail status
        status_ = s;
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

}  // namespace sdk

}  // namespace dingodb
