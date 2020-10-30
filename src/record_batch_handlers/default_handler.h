#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include <arrow/api.h>

#include "record_batch_handler.h"

#include "metadata.pb.h"

class DefaultHandler : public RecordBatchHandler {
 public:
  template <typename ValueType>
  struct DefaultCase {
    ValueType default_value;
    ColumnType default_column_type{UNKNOWN};
  };

  struct DefaultHandlerOptions {
    std::unordered_map<std::string, DefaultCase<int64_t>> int64_default_cases;

    std::unordered_map<std::string, DefaultCase<double>> double_default_cases;

    std::unordered_map<std::string, DefaultCase<std::string>>
        string_default_cases;

    std::unordered_map<std::string, DefaultCase<bool>> bool_default_cases;
  };

  template <typename DefaultHandlerOptionsType>
  explicit DefaultHandler(DefaultHandlerOptionsType&& options)
      : options_(std::forward<DefaultHandlerOptionsType>(options)) {}

  arrow::Status handle(
      const std::shared_ptr<arrow::RecordBatch>& record_batch,
      arrow::RecordBatchVector* result) override;

 private:
  template <typename T>
  arrow::Status addMissingColumn(
      const std::unordered_map<std::string, DefaultCase<T>>& default_cases,
      std::shared_ptr<arrow::RecordBatch>* record_batch) const;

 private:
  DefaultHandlerOptions options_;
};
