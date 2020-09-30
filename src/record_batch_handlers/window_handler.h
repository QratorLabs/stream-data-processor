#pragma once

#include "record_batch_handler.h"

class WindowHandler : public RecordBatchHandler {
 public:
  arrow::Status handle(const arrow::RecordBatchVector& record_batches, arrow::RecordBatchVector* result) override;
};
