#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "metadata.pb.h"

class RecordBatchGrouping {
 public:
  static arrow::Status fillGroupMetadata(
      std::shared_ptr<arrow::RecordBatch>* record_batch,
      const std::vector<std::string>& grouping_columns);

  static std::string extractGroupMetadata(
      const std::shared_ptr<arrow::RecordBatch>& record_batch);

  static RecordBatchGroup extractGroup(
      const std::shared_ptr<arrow::RecordBatch>& record_batch);

  static std::vector<std::string> extractGroupingColumnsNames(
      const std::shared_ptr<arrow::RecordBatch>& record_batch);

  static std::string getGroupingColumnsSetKey(
      const std::shared_ptr<arrow::RecordBatch>& record_batch);

  static RecordBatchGroup constructGroupFromOrderedMap(
      const std::map<std::string, std::string>& group_map);

  static arrow::Status setGroupMetadata(
      std::shared_ptr<arrow::RecordBatch>* record_batch,
      const RecordBatchGroup& group);

 private:
  static arrow::Status fillGroupMap(
      std::map<std::string, std::string>* group_map,
      const std::shared_ptr<arrow::RecordBatch>& record_batch,
      const std::vector<std::string>& grouping_columns);

 private:
  static const std::string GROUP_METADATA_KEY;
};
