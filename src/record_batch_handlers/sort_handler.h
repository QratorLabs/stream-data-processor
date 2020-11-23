#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "record_batch_handler.h"

namespace stream_data_processor {

class SortHandler : public RecordBatchHandler {
 public:
  template <typename StringVectorType>
  explicit SortHandler(StringVectorType&& sort_by_columns)
      : sort_by_columns_(std::forward<StringVectorType>(sort_by_columns)) {}

  arrow::Result<arrow::RecordBatchVector> handle(
      const std::shared_ptr<arrow::RecordBatch>& record_batch) override;

 private:
  std::vector<std::string> sort_by_columns_;
};

}  // namespace stream_data_processor
