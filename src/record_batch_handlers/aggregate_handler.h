#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "aggregate_functions/aggregate_function.h"
#include "record_batch_handler.h"

class AggregateHandler : public RecordBatchHandler {
 public:
  enum AggregateFunctionEnumType { kFirst, kLast, kMax, kMin, kMean };

  struct AggregateCase {
    AggregateFunctionEnumType aggregate_function;
    std::string result_column_name;
  };

  struct AggregateOptions {
    std::unordered_map<std::string, std::vector<AggregateCase>>
        aggregate_columns;
    std::vector<std::string> grouping_columns;
    std::string time_column_name;
    bool add_result_time_column{false};
    AggregateCase result_time_column_rule{kLast, "time"};
  };

  explicit AggregateHandler(const AggregateOptions& options);
  explicit AggregateHandler(AggregateOptions&& options);

  arrow::Status handle(const arrow::RecordBatchVector& record_batches,
                       arrow::RecordBatchVector* result) override;

 private:
  arrow::Status fillResultSchema(
      const std::shared_ptr<arrow::RecordBatch>& record_batch);
  arrow::Status fillGroupingColumns(const arrow::RecordBatchVector& groups,
                                    arrow::ArrayVector* result_arrays) const;

  arrow::Status aggregate(
      const arrow::RecordBatchVector& groups,
      const std::string& aggregate_column_name,
      const AggregateFunctionEnumType& aggregate_function,
      arrow::ArrayVector* result_arrays,
      arrow::MemoryPool* pool = arrow::default_memory_pool()) const;

  arrow::Status aggregateTimeColumn(
      const arrow::RecordBatchVector& groups,
      const AggregateFunctionEnumType& aggregate_function,
      arrow::ArrayVector* result_arrays,
      arrow::MemoryPool* pool = arrow::default_memory_pool()) const;

 private:
  AggregateOptions options_;
  std::shared_ptr<arrow::Schema> result_schema_{nullptr};

  static const std::unordered_map<AggregateFunctionEnumType,
                                  std::shared_ptr<AggregateFunction>>
      TYPES_TO_FUNCTIONS;
};
