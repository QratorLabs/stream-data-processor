#pragma once

#include "aggregate_function.h"

class MeanAggregateFunction : public AggregateFunction {
 public:
  arrow::Status aggregate(const std::shared_ptr<arrow::RecordBatch> &data,
                          const std::string &column_name,
                          std::shared_ptr<arrow::Scalar> *result,
                          const std::string &ts_column_name = "") const override;
};


