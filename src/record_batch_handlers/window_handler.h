#pragma once

#include "record_batch_handler.h"

namespace stream_data_processor {

class WindowHandler : public RecordBatchHandler {
 public:
  arrow::Status handle(
      const std::shared_ptr<arrow::RecordBatch>& record_batch,
      arrow::RecordBatchVector* result) override;

  arrow::Status handle(const arrow::RecordBatchVector& record_batches,
                       arrow::RecordBatchVector* result) override;
};

}  // namespace stream_data_processor
