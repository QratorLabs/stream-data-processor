#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "aggregate_functions/aggregate_function.h"
#include "record_batch_handler.h"

class AggregateHandler : public RecordBatchHandler {
 public:
  struct AggregateOptions {
    std::unordered_map<std::string, std::vector<std::string>> aggregate_columns;
    bool add_time_window_columns_{false};
  };

  template <typename StringVector, typename AggregateOptionsType>
  AggregateHandler(StringVector&& grouping_columns, AggregateOptionsType&& options, std::string ts_column_name = "")
      : grouping_columns_(std::forward<StringVector>(grouping_columns))
      , options_(std::forward<AggregateOptionsType>(options))
      , ts_column_name_(std::move(ts_column_name)) {

  }

  arrow::Status handle(const arrow::RecordBatchVector& record_batches, arrow::RecordBatchVector* result) override;

 private:
  arrow::Status findTsColumnName(const std::shared_ptr<arrow::RecordBatch>& record_batch);
  arrow::Status fillResultSchema(const std::shared_ptr<arrow::RecordBatch>& record_batch);
  arrow::Status fillGroupingColumns(const arrow::RecordBatchVector& groups, arrow::ArrayVector *result_arrays) const;

  arrow::Status aggregate(const arrow::RecordBatchVector& groups, const std::string& aggregate_column_name,
      const std::string& aggregate_function, arrow::ArrayVector *result_arrays,
      arrow::MemoryPool *pool = arrow::default_memory_pool()) const;

  arrow::Status aggregateTsColumn(const arrow::RecordBatchVector& groups, const std::string& aggregate_function,
      arrow::ArrayVector *result_arrays, arrow::MemoryPool *pool = arrow::default_memory_pool()) const;

 private:
  std::vector<std::string> grouping_columns_;
  AggregateOptions options_;
  std::string ts_column_name_;
  std::shared_ptr<arrow::Schema> result_schema_{nullptr};

  static const std::unordered_map<std::string, std::shared_ptr<AggregateFunction>> NAMES_TO_FUNCTIONS;
};
