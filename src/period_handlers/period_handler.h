#pragma once

#include <deque>

#include <arrow/api.h>

class PeriodHandler {
 public:
  virtual arrow::Status handle(const std::deque<std::shared_ptr<arrow::Buffer>> &period,
                               std::shared_ptr<arrow::Buffer> &result) = 0;
};