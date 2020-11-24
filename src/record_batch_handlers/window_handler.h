#pragma once

#include "record_batch_handler.h"

namespace stream_data_processor {

class WindowHandler : public RecordBatchHandler {
 public:
  arrow::Result<arrow::RecordBatchVector> handle(
      const std::shared_ptr<arrow::RecordBatch>& record_batch) override;

  arrow::Result<arrow::RecordBatchVector> handle(
      const arrow::RecordBatchVector& record_batches) override;
};

}  // namespace stream_data_processor
