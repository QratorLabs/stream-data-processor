#pragma once

#include <memory>
#include <vector>

#include <arrow/api.h>

class RecordBatchHandler {
 public:
  virtual arrow::Status handle(
      const std::shared_ptr<arrow::RecordBatch>& record_batch,
      arrow::RecordBatchVector* result) = 0;

  virtual arrow::Status handle(const arrow::RecordBatchVector& record_batches,
                               arrow::RecordBatchVector* result) {
    for (auto& record_batch : record_batches) {
      ARROW_RETURN_NOT_OK(handle(record_batch, result));
    }

    return arrow::Status::OK();
  }

 protected:
  void copyMetadata(const std::shared_ptr<arrow::RecordBatch>& from,
                    std::shared_ptr<arrow::RecordBatch>* to) {
    if (from->schema()->HasMetadata()) {
      *to = to->get()->ReplaceSchemaMetadata(from->schema()->metadata());
    }
  }
};
