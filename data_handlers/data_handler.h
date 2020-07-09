#pragma once

#include <arrow/api.h>

class DataHandler {
 public:
  virtual arrow::Status handle(std::shared_ptr<arrow::Buffer> source, std::shared_ptr<arrow::Buffer>* target) = 0;
};
