#pragma once

#include "aggregate_function.h"

class MaxAggregateFunction : public AggregateFunction {
 public:
  arrow::Status aggregate(
      const std::shared_ptr<arrow::RecordBatch>& data,
      const std::string& column_name,
      std::shared_ptr<arrow::Scalar>* result) const override;
};
