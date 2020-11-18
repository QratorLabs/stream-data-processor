#pragma once

#include <memory>
#include <string>

#include <arrow/api.h>

namespace stream_data_processor {

class AggregateFunction {
 public:
  virtual arrow::Status aggregate(
      const arrow::RecordBatch& data, const std::string& column_name,
      std::shared_ptr<arrow::Scalar>* result) const = 0;
};

}  // namespace stream_data_processor
