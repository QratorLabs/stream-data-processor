#include "serialized_record_batch_handler.h"

#include "utils/serializer.h"

SerializedRecordBatchHandler::SerializedRecordBatchHandler(
    std::shared_ptr<RecordBatchHandler> handler_strategy)
    : handler_strategy_(std::move(handler_strategy)) {}

arrow::Status SerializedRecordBatchHandler::handle(
    const std::shared_ptr<arrow::Buffer>& source,
    std::shared_ptr<arrow::Buffer>* target) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;
  ARROW_RETURN_NOT_OK(
      Serializer::deserializeRecordBatches(source, &record_batches));
  if (record_batches.empty()) {
    return arrow::Status::CapacityError("No data to handle");
  }

  arrow::RecordBatchVector result;
  ARROW_RETURN_NOT_OK(handler_strategy_->handle(record_batches, &result));

  ARROW_RETURN_NOT_OK(Serializer::serializeRecordBatches(
      result.back()->schema(), result, target));
  return arrow::Status::OK();
}
