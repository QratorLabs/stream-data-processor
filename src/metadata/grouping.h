#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "metadata.pb.h"

namespace stream_data_processor {
namespace metadata {

arrow::Status fillGroupMetadata(
    std::shared_ptr<arrow::RecordBatch>* record_batch,
    const std::vector<std::string>& grouping_columns);

std::string extractGroupMetadata(const arrow::RecordBatch& record_batch);

RecordBatchGroup extractGroup(const arrow::RecordBatch& record_batch);

std::vector<std::string> extractGroupingColumnsNames(
    const arrow::RecordBatch& record_batch);

std::string getGroupingColumnsSetKey(const arrow::RecordBatch& record_batch);

RecordBatchGroup constructGroupFromOrderedMap(
    const std::map<std::string, std::string>& group_map);

arrow::Status setGroupMetadata(
    std::shared_ptr<arrow::RecordBatch>* record_batch,
    const RecordBatchGroup& group);

}  // namespace metadata
}  // namespace stream_data_processor
