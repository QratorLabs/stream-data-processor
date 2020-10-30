#pragma once

#include <memory>
#include <vector>

#include <arrow/api.h>

class DataHandler {
 public:
  virtual arrow::Status handle(
      const std::shared_ptr<arrow::Buffer>& source,
      std::vector<std::shared_ptr<arrow::Buffer>>* target) = 0;
};
