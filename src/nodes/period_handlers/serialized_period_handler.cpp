#include "serialized_period_handler.h"

#include "utils/serialize_utils.h"

namespace stream_data_processor {

SerializedPeriodHandler::SerializedPeriodHandler(
    std::shared_ptr<RecordBatchHandler> handler_strategy)
    : handler_strategy_(std::move(handler_strategy)) {}

arrow::Result<arrow::BufferVector> SerializedPeriodHandler::handle(
    const std::deque<std::shared_ptr<arrow::Buffer>>& period) {
  arrow::RecordBatchVector period_vector;
  for (auto& buffer : period) {
    ARROW_ASSIGN_OR_RAISE(period_vector,
                          serialize_utils::deserializeRecordBatches(*buffer));
  }

  if (period_vector.empty()) {
    return arrow::Status::OK();
  }

  arrow::RecordBatchVector result;
  ARROW_ASSIGN_OR_RAISE(result, handler_strategy_->handle(period_vector));
  return serialize_utils::serializeRecordBatches(result);
}

}  // namespace stream_data_processor
