#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "data_handler.h"

class SortHandler : public DataHandler {
 public:
  template <typename U>
  explicit SortHandler(U&& column_names) : column_names_(std::forward<U>(column_names)) {

  }

  arrow::Status handle(const std::shared_ptr<arrow::Buffer> &source, std::shared_ptr<arrow::Buffer>* target) override;

 private:
  std::vector<std::string> column_names_;
};


