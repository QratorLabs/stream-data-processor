#pragma once

#include <memory>
#include <string>

#include <arrow/api.h>

namespace stream_data_processor {

class AggregateFunction {
 public:
  AggregateFunction() = default;

  AggregateFunction(const AggregateFunction& /* non-used*/) = default;
  AggregateFunction& operator=(const AggregateFunction& /* non-used*/) =
      default;

  AggregateFunction(AggregateFunction&& /* non-used*/) = default;
  AggregateFunction& operator=(AggregateFunction&& /* non-used*/) = default;

  [[nodiscard]] virtual arrow::Result<std::shared_ptr<arrow::Scalar>>
  aggregate(const arrow::RecordBatch& data,
            const std::string& column_name) const = 0;

  virtual ~AggregateFunction() = 0;
};

}  // namespace stream_data_processor
