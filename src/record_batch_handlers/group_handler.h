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

  arrow::Status handle(
      const std::shared_ptr<arrow::RecordBatch>& record_batch,
      arrow::RecordBatchVector* result) override;

 private:
  std::vector<std::string> grouping_columns_;
};

}  // namespace stream_data_processor
