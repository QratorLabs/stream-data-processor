#pragma once

#include <string>
#include <vector>

#include "data_handler.h"

class GroupHandler : public DataHandler {
 public:
  template <typename U>
  explicit GroupHandler(U&& grouping_columns)
      : grouping_columns_(std::forward<U>(grouping_columns)) {

  }

  arrow::Status handle(const std::shared_ptr<arrow::Buffer> &source, std::shared_ptr<arrow::Buffer>* target) override;

 private:
  std::vector<std::string> grouping_columns_;
};
