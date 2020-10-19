#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "record_batch_handler.h"

class SortHandler : public RecordBatchHandler {
 public:
  template <typename StringVectorType>
  explicit SortHandler(StringVectorType&& sort_by_columns)
      : sort_by_columns_(std::forward<StringVectorType>(sort_by_columns)) {}

  arrow::Status handle(const arrow::RecordBatchVector& record_batches,
                       arrow::RecordBatchVector* result) override;

 private:
  std::vector<std::string> sort_by_columns_;
};
