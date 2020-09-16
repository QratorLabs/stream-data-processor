#pragma once

#include <memory>

#include <gandiva/condition.h>
#include <gandiva/filter.h>

#include "record_batch_handler.h"

class FilterHandler : public RecordBatchHandler {
 public:
  template <typename U>
  explicit FilterHandler(U&& conditions)
      : conditions_(std::forward<U>(conditions)) {

  }

  arrow::Status handle(const arrow::RecordBatchVector& record_batches, arrow::RecordBatchVector& result) override;

 private:
  arrow::Status prepareFilter(const std::shared_ptr<arrow::Schema>& schema, std::shared_ptr<gandiva::Filter>& filter) const;

 private:
  std::vector<gandiva::ConditionPtr> conditions_;
};


