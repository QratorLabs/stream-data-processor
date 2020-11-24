#pragma once

#include <deque>

#include <arrow/api.h>

namespace stream_data_processor {

class PeriodHandler {
 public:
  [[nodiscard]] virtual arrow::Result<arrow::BufferVector> handle(
      const std::deque<std::shared_ptr<arrow::Buffer>>& period) = 0;
};

}  // namespace stream_data_processor
