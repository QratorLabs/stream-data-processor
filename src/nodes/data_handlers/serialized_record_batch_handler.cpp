#include <spdlog/spdlog.h>

#include "serialized_record_batch_handler.h"

#include "utils/serialize_utils.h"

namespace stream_data_processor {

SerializedRecordBatchHandler::SerializedRecordBatchHandler(
    std::shared_ptr<RecordBatchHandler> handler_strategy)
    : handler_strategy_(std::move(handler_strategy)) {}

arrow::Status SerializedRecordBatchHandler::handle(
    const std::shared_ptr<arrow::Buffer>& source,
    std::vector<std::shared_ptr<arrow::Buffer>>* target) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;

  ARROW_RETURN_NOT_OK(
      serialize_utils::deserializeRecordBatches(source, &record_batches));

  if (record_batches.empty()) {
    return arrow::Status::OK();
  }

  arrow::RecordBatchVector result;
  ARROW_RETURN_NOT_OK(handler_strategy_->handle(record_batches, &result));

  ARROW_RETURN_NOT_OK(
      serialize_utils::serializeRecordBatches(result, target));
  return arrow::Status::OK();
}

}  // namespace stream_data_processor
