#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include <arrow/api.h>

#include "data_handler.h"

class DefaultHandler : public DataHandler {
 public:
  struct DefaultHandlerOptions {
    std::unordered_map<std::string, int64_t> int64_columns_default_values;
    std::unordered_map<std::string, double> double_columns_default_values;
    std::unordered_map<std::string, std::string> string_columns_default_values;
  };

  template <typename U>
  explicit DefaultHandler(U&& options)
      : options_(std::forward<U>(options)) {

  }

  arrow::Status handle(const std::shared_ptr<arrow::Buffer> &source, std::shared_ptr<arrow::Buffer>* target) override;

 private:
  template <typename T>
  arrow::Status addMissingColumn(const std::unordered_map<std::string, T> &missing_columns,
                                 arrow::RecordBatchVector &record_batches) const;

 private:
  DefaultHandlerOptions options_;
};
