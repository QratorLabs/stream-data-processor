#include "serialized_period_handler.h"

#include "utils/serialize_utils.h"

namespace stream_data_processor {

SerializedPeriodHandler::SerializedPeriodHandler(
    std::shared_ptr<RecordBatchHandler> handler_strategy)
    : handler_strategy_(std::move(handler_strategy)) {}

arrow::Status SerializedPeriodHandler::handle(
    const std::deque<std::shared_ptr<arrow::Buffer>>& period,
    std::vector<std::shared_ptr<arrow::Buffer>>* target) {
  arrow::RecordBatchVector period_vector;
  for (auto& buffer : period) {
    ARROW_RETURN_NOT_OK(
        serialize_utils::deserializeRecordBatches(buffer, &period_vector));
  }

  if (period_vector.empty()) {
    return arrow::Status::OK();
  }

  arrow::RecordBatchVector result;
  ARROW_RETURN_NOT_OK(handler_strategy_->handle(period_vector, &result));

  ARROW_RETURN_NOT_OK(
      serialize_utils::serializeRecordBatches(result, target));
  return arrow::Status::OK();
}

}  // namespace stream_data_processor
