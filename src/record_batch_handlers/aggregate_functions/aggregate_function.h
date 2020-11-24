#pragma once

#include <memory>
#include <string>

#include <arrow/api.h>

namespace stream_data_processor {

class AggregateFunction {
 public:
  [[nodiscard]] virtual arrow::Result<std::shared_ptr<arrow::Scalar>>
  aggregate(const arrow::RecordBatch& data,
            const std::string& column_name) const = 0;

  virtual ~AggregateFunction() = default;
};

}  // namespace stream_data_processor
