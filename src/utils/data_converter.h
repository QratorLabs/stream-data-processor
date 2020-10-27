#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include <arrow/api.h>

class DataConverter {
 public:
  static arrow::Status convertTableToRecordBatch(
      const std::shared_ptr<arrow::Table>& table,
      std::shared_ptr<arrow::RecordBatch>* record_batch);

  static arrow::Status concatenateRecordBatches(
      const std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batches,
      std::shared_ptr<arrow::RecordBatch>* target);
};
