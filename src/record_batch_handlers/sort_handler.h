#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "record_batch_handler.h"

class SortHandler : public RecordBatchHandler {
 public:
  template <typename U>
  explicit SortHandler(U&& column_names) : column_names_(std::forward<U>(column_names)) {

  }

  arrow::Status handle(const arrow::RecordBatchVector& record_batches, arrow::RecordBatchVector& result) override;

 private:
  std::vector<std::string> column_names_;
};


