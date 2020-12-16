#include "log_handler.h"

namespace stream_data_processor {

LogHandler::LogHandler(const spdlog::level::level_enum& log_level)
    : log_level_(log_level) {}

arrow::Result<arrow::RecordBatchVector> LogHandler::handle(
    const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  spdlog::log(log_level_, "RecordBatch schema:\n{}\n\nRecordBatch:\n{}",
              record_batch->schema()->ToString(true),
              record_batch->ToString());

  return std::vector{record_batch};
}

}  // namespace stream_data_processor
