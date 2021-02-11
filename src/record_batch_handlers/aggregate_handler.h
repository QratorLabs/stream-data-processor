#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "aggregate_functions/aggregate_function.h"
#include "record_batch_handler.h"

#include "metadata.pb.h"

namespace stream_data_processor {

class AggregateHandler : public RecordBatchHandler {
 public:
  enum AggregateFunctionEnumType { kFirst, kLast, kMax, kMin, kMean };

  struct AggregateCase {
    AggregateFunctionEnumType aggregate_function;
    std::string result_column_name;
    metadata::ColumnType result_column_type{metadata::FIELD};
  };

  struct AggregateOptions {
    std::unordered_map<std::string, std::vector<AggregateCase>>
        aggregate_columns;
    AggregateCase result_time_column_rule{kLast, "time", metadata::TIME};
  };

  explicit AggregateHandler(const AggregateOptions& options);
  explicit AggregateHandler(AggregateOptions&& options);

  arrow::Result<arrow::RecordBatchVector> handle(
      const std::shared_ptr<arrow::RecordBatch>& record_batch) override;

  arrow::Result<arrow::RecordBatchVector> handle(
      const arrow::RecordBatchVector& record_batches) override;

 private:
  static std::unordered_map<std::string, arrow::RecordBatchVector>
  splitByGroups(const arrow::RecordBatchVector& record_batches);

  arrow::Status isValid(const arrow::RecordBatchVector& record_batches) const;

  arrow::Result<std::shared_ptr<arrow::Schema>> createResultSchema(
      const arrow::RecordBatchVector& record_batches,
      const std::vector<std::string>& grouping_columns,
      bool explicitly_add_measurement,
      const std::string& measurement_column_name,
      const std::string& time_column_name) const;

  static arrow::Status fillGroupingColumns(
      const arrow::RecordBatchVector& grouped,
      arrow::ArrayVector* result_arrays,
      const std::vector<std::string>& grouping_columns);

  arrow::Status aggregate(
      const arrow::RecordBatchVector& groups,
      const std::string& aggregate_column_name,
      const AggregateFunctionEnumType& aggregate_function,
      arrow::ArrayVector* result_arrays,
      arrow::MemoryPool* pool = arrow::default_memory_pool()) const;

  arrow::Status aggregateTimeColumn(
      const arrow::RecordBatchVector& record_batch_vector,
      arrow::ArrayVector* result_arrays,
      arrow::MemoryPool* pool = arrow::default_memory_pool()) const;

  static arrow::Status fillMeasurementColumn(
      const arrow::RecordBatchVector& grouped,
      arrow::ArrayVector* result_arrays,
      const std::string& measurement_column_name);

 private:
  static const std::unordered_map<AggregateFunctionEnumType,
                                  std::unique_ptr<AggregateFunction>>
      TYPES_TO_FUNCTIONS;

  AggregateOptions options_;
};

}  // namespace stream_data_processor
