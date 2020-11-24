#pragma once

#include <string>
#include <vector>

#include "record_batch_handler.h"

namespace stream_data_processor {

class GroupHandler : public RecordBatchHandler {
 public:
  template <typename StringVectorType>
  explicit GroupHandler(StringVectorType&& grouping_columns)
      : grouping_columns_(std::forward<StringVectorType>(grouping_columns)) {}

  arrow::Result<arrow::RecordBatchVector> handle(
      const std::shared_ptr<arrow::RecordBatch>& record_batch) override;

 private:
  std::vector<std::string> grouping_columns_;
};

}  // namespace stream_data_processor
