#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "grouping.pb.h"

class RecordBatchGrouping {
 public:
  static arrow::Status fillGroupMetadata(
      std::shared_ptr<arrow::RecordBatch>* record_batch,
      const std::vector<std::string>& grouping_columns);

  static std::string extractGroupMetadata(
      const std::shared_ptr<arrow::RecordBatch>& record_batch);

  static RecordBatchGroup extractGroup(
      const std::shared_ptr<arrow::RecordBatch>& record_batch);

 private:
  static const std::string METADATA_KEY;
};
