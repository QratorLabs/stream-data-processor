#include "serialized_period_handler.h"

#include "utils/serializer.h"

SerializedPeriodHandler::SerializedPeriodHandler(std::shared_ptr<RecordBatchHandler> handler_strategy)
    : handler_strategy_(std::move(handler_strategy)) {

}

arrow::Status SerializedPeriodHandler::handle(const std::deque<std::shared_ptr<arrow::Buffer>> &period,
                                              std::shared_ptr<arrow::Buffer> &target) {
  arrow::RecordBatchVector period_vector;
  for (auto& buffer : period) {
    arrow::RecordBatchVector record_batch_vector;
    ARROW_RETURN_NOT_OK(Serializer::deserializeRecordBatches(buffer, &record_batch_vector));
    for (auto& record_batch : record_batch_vector) {
      period_vector.push_back(record_batch);
    }
  }

  if (period_vector.empty()) {
    return arrow::Status::CapacityError("No data to handle");
  }

  arrow::RecordBatchVector result;
  ARROW_RETURN_NOT_OK(handler_strategy_->handle(period_vector, result));

  ARROW_RETURN_NOT_OK(Serializer::serializeRecordBatches(result.back()->schema(), result, &target));
  return arrow::Status::OK();
}
