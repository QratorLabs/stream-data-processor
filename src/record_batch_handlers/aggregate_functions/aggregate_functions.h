#pragma once

#include "aggregate_function.h"

namespace stream_data_processor {

class FirstAggregateFunction : public AggregateFunction {
 public:
  [[nodiscard]] arrow::Result<std::shared_ptr<arrow::Scalar>> aggregate(
      const arrow::RecordBatch& data,
      const std::string& column_name) const override;
};

class LastAggregateFunction : public AggregateFunction {
 public:
  [[nodiscard]] arrow::Result<std::shared_ptr<arrow::Scalar>> aggregate(
      const arrow::RecordBatch& data,
      const std::string& column_name) const override;
};

class MaxAggregateFunction : public AggregateFunction {
 public:
  [[nodiscard]] arrow::Result<std::shared_ptr<arrow::Scalar>> aggregate(
      const arrow::RecordBatch& data,
      const std::string& column_name) const override;
};

class MeanAggregateFunction : public AggregateFunction {
 public:
  [[nodiscard]] arrow::Result<std::shared_ptr<arrow::Scalar>> aggregate(
      const arrow::RecordBatch& data,
      const std::string& column_name) const override;
};

class MinAggregateFunction : public AggregateFunction {
 public:
  [[nodiscard]] arrow::Result<std::shared_ptr<arrow::Scalar>> aggregate(
      const arrow::RecordBatch& data,
      const std::string& column_name) const override;
};

}  // namespace stream_data_processor