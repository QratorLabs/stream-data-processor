#pragma once

#include <memory>

#include <gandiva/condition.h>
#include <gandiva/filter.h>

#include "record_batch_handler.h"

namespace stream_data_processor {

class FilterHandler : public RecordBatchHandler {
 public:
  template <typename ConditionVectorType>
  explicit FilterHandler(ConditionVectorType&& conditions)
      : conditions_(std::forward<ConditionVectorType>(conditions)) {}

  arrow::Result<arrow::RecordBatchVector> handle(
      const std::shared_ptr<arrow::RecordBatch>& record_batch) override;

 private:
  arrow::Result<std::shared_ptr<gandiva::Filter>> createFilter(
      const std::shared_ptr<arrow::Schema>& schema) const;

 private:
  std::vector<gandiva::ConditionPtr> conditions_;
};

}  // namespace stream_data_processor
