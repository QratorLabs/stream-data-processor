#pragma once

#include "aggregate_function.h"

namespace stream_data_processor {

class FirstAggregateFunction : public AggregateFunction {
 public:
  arrow::Status aggregate(
      const std::shared_ptr<arrow::RecordBatch>& data,
      const std::string& column_name,
      std::shared_ptr<arrow::Scalar>* result) const override;
};

class LastAggregateFunction : public AggregateFunction {
 public:
  arrow::Status aggregate(
      const std::shared_ptr<arrow::RecordBatch>& data,
      const std::string& column_name,
      std::shared_ptr<arrow::Scalar>* result) const override;
};

class MaxAggregateFunction : public AggregateFunction {
 public:
  arrow::Status aggregate(
      const std::shared_ptr<arrow::RecordBatch>& data,
      const std::string& column_name,
      std::shared_ptr<arrow::Scalar>* result) const override;
};

class MeanAggregateFunction : public AggregateFunction {
 public:
  arrow::Status aggregate(
      const std::shared_ptr<arrow::RecordBatch>& data,
      const std::string& column_name,
      std::shared_ptr<arrow::Scalar>* result) const override;
};

class MinAggregateFunction : public AggregateFunction {
 public:
  arrow::Status aggregate(
      const std::shared_ptr<arrow::RecordBatch>& data,
      const std::string& column_name,
      std::shared_ptr<arrow::Scalar>* result) const override;
};

}  // namespace stream_data_processor