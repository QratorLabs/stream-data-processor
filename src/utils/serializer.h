#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

class Serializer {
 public:
  static arrow::Status serializeRecordBatches(
      const std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batches,
      std::vector<std::shared_ptr<arrow::Buffer>>* target);

  static arrow::Status deserializeRecordBatches(
      const std::shared_ptr<arrow::Buffer>& buffer,
      std::vector<std::shared_ptr<arrow::RecordBatch>>* record_batches);
};
