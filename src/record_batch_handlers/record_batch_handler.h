#pragma once

#include <vector>

#include <arrow/api.h>

class RecordBatchHandler {
 public:
  virtual arrow::Status handle(const arrow::RecordBatchVector& record_batches, arrow::RecordBatchVector& result) = 0;
};