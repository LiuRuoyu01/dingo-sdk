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

#ifndef DINGODB_SDK_DOCUMENT_INDEX_CACHE_H_
#define DINGODB_SDK_DOCUMENT_INDEX_CACHE_H_

#include <cstdint>
#include <shared_mutex>
#include <unordered_map>

#include "common/logging.h"
#include "dingosdk/vector.h"
#include "sdk/document/document_index.h"
#include "sdk/utils/rw_lock.h"

namespace dingodb {
namespace sdk {

using DocumentIndexCacheKey = std::string;

class DocumentIndexCache {
 public:
  DocumentIndexCache(const DocumentIndexCache&) = delete;
  const DocumentIndexCache& operator=(const DocumentIndexCache&) = delete;

  explicit DocumentIndexCache(const ClientStub& stub);

  ~DocumentIndexCache() = default;

  Status GetIndexIdByKey(const DocumentIndexCacheKey& index_key, int64_t& index_id);

  Status GetDocumentIndexByKey(const DocumentIndexCacheKey& index_key, std::shared_ptr<DocumentIndex>& out_doc_index);

  Status GetDocumentIndexById(int64_t index_id, std::shared_ptr<DocumentIndex>& out_doc_index);

  void RemoveDocumentIndexById(int64_t index_id);

  void RemoveDocumentIndexByKey(const DocumentIndexCacheKey& index_key);

 private:
  Status SlowGetDocumentIndexByKey(const DocumentIndexCacheKey& index_key,
                                   std::shared_ptr<DocumentIndex>& out_doc_index);
  Status SlowGetDocumentIndexById(int64_t index_id, std::shared_ptr<DocumentIndex>& out_doc_index);
  Status ProcessIndexDefinitionWithId(const pb::meta::IndexDefinitionWithId& index_def_with_id,
                                      std::shared_ptr<DocumentIndex>& out_doc_index);

  static bool CheckIndexDefinitionWithId(const pb::meta::IndexDefinitionWithId& index_def_with_id);
  template <class DocumentIndexResponse>
  static bool CheckIndexResponse(const DocumentIndexResponse& response);

  const ClientStub& stub_;
  RWLock rw_lock_;
  std::unordered_map<DocumentIndexCacheKey, int64_t> index_key_to_id_;
  std::unordered_map<int64_t, std::shared_ptr<DocumentIndex>> id_to_index_;
};

template <class DocumentIndexResponse>
bool DocumentIndexCache::CheckIndexResponse(const DocumentIndexResponse& response) {
  bool checked = true;
  if (!response.has_index_definition_with_id()) {
    checked = false;
  } else {
    checked = CheckIndexDefinitionWithId(response.index_definition_with_id());
  }

  if (!checked) {
    DINGO_LOG(WARNING) << "Fail checked, response:" << response.DebugString();
  }

  return checked;
}

static DocumentIndexCacheKey EncodeDocumentIndexCacheKey(int64_t schema_id, const std::string& index_name) {
  DCHECK_GT(schema_id, 0);
  DCHECK(!index_name.empty());
  auto buf_size = sizeof(schema_id) + index_name.size();
  char buf[buf_size];
  memcpy(buf, &schema_id, sizeof(schema_id));
  memcpy(buf + sizeof(schema_id), index_name.data(), index_name.size());
  std::string tmp(buf, buf_size);
  return std::move(tmp);
}

static void DecodeDocumentIndexCacheKey(const DocumentIndexCacheKey& key, int64_t& schema_id, std::string& index_name) {
  DCHECK_GE(key.size(), sizeof(schema_id));
  int64_t tmp_schema_id;
  memcpy(&tmp_schema_id, key.data(), sizeof(schema_id));
  schema_id = tmp_schema_id;
  index_name = std::string(key.data() + sizeof(schema_id), key.size() - sizeof(schema_id));
}

static DocumentIndexCacheKey GetDocumentIndexCacheKey(const DocumentIndex& index) {
  return std::move(EncodeDocumentIndexCacheKey(index.GetSchemaId(), index.GetName()));
}

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_DOCUMENT_INDEX_CACHE_H_