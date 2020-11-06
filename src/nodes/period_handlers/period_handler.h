#pragma once

#include <deque>

#include <arrow/api.h>

namespace stream_data_processor {

class PeriodHandler {
 public:
  virtual arrow::Status handle(
      const std::deque<std::shared_ptr<arrow::Buffer>>& period,
      std::vector<std::shared_ptr<arrow::Buffer>>* target) = 0;
};

}  // namespace stream_data_processor
