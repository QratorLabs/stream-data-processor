#include "serialized_period_handler.h"

#include "utils/serializer.h"

SerializedPeriodHandler::SerializedPeriodHandler(
    std::shared_ptr<RecordBatchHandler> handler_strategy)
    : handler_strategy_(std::move(handler_strategy)) {}

arrow::Status SerializedPeriodHandler::handle(
    const std::deque<std::shared_ptr<arrow::Buffer>>& period,
    std::vector<std::shared_ptr<arrow::Buffer>>* target) {
  arrow::RecordBatchVector period_vector;
  for (auto& buffer : period) {
    ARROW_RETURN_NOT_OK(
        Serializer::deserializeRecordBatches(buffer, &period_vector));
  }

  if (period_vector.empty()) {
    return arrow::Status::OK();
  }

  arrow::RecordBatchVector result;
  ARROW_RETURN_NOT_OK(handler_strategy_->handle(period_vector, &result));

  ARROW_RETURN_NOT_OK(Serializer::serializeRecordBatches(result, target));
  return arrow::Status::OK();
}
