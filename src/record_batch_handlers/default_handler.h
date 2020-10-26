#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include <arrow/api.h>

#include "record_batch_handler.h"

class DefaultHandler : public RecordBatchHandler {
 public:
  struct DefaultHandlerOptions {
    std::unordered_map<std::string, int64_t> int64_columns_default_values;
    std::unordered_map<std::string, double> double_columns_default_values;
    std::unordered_map<std::string, std::string>
        string_columns_default_values;
    std::unordered_map<std::string, bool> bool_columns_default_values;
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
      const std::unordered_map<std::string, T>& missing_columns,
      std::shared_ptr<arrow::RecordBatch>* record_batch) const;

 private:
  DefaultHandlerOptions options_;
};
