#pragma once

#include <memory>
#include <string>

#include <arrow/api.h>

class AggregateFunction {
 public:
  virtual arrow::Status aggregate(const std::shared_ptr<arrow::RecordBatch> &data,
                                  const std::string &column_name,
                                  std::shared_ptr<arrow::Scalar> *result,
                                  const std::string &ts_column_name = "") const = 0;
};
