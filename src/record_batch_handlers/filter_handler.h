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

  arrow::Status handle(
      const std::shared_ptr<arrow::RecordBatch>& record_batch,
      arrow::RecordBatchVector* result) override;

 private:
  arrow::Status prepareFilter(const std::shared_ptr<arrow::Schema>& schema,
                              std::shared_ptr<gandiva::Filter>* filter) const;

 private:
  std::vector<gandiva::ConditionPtr> conditions_;
};

}  // namespace stream_data_processor
