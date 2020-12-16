#pragma once

#include <spdlog/spdlog.h>

#include "record_batch_handler.h"

namespace stream_data_processor {

class LogHandler : public RecordBatchHandler {
 public:
  explicit LogHandler(
      const spdlog::level::level_enum& log_level = spdlog::level::info);

  arrow::Result<arrow::RecordBatchVector> handle(
      const std::shared_ptr<arrow::RecordBatch>& record_batch) override;

 private:
  spdlog::level::level_enum log_level_;
};

}  // namespace stream_data_processor
