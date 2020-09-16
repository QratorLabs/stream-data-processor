#pragma once

#include <string>
#include <vector>

#include "record_batch_handler.h"

class GroupHandler : public RecordBatchHandler {
 public:
  template <typename U>
  explicit GroupHandler(U&& grouping_columns)
      : grouping_columns_(std::forward<U>(grouping_columns)) {

  }

  arrow::Status handle(const arrow::RecordBatchVector& record_batches, arrow::RecordBatchVector& result) override;

 private:
  std::vector<std::string> grouping_columns_;
};
