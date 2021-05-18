#include <spdlog/spdlog.h>

#include "serialized_record_batch_handler.h"

#include "utils/serialize_utils.h"

namespace stream_data_processor {

SerializedRecordBatchHandler::SerializedRecordBatchHandler(
    std::shared_ptr<RecordBatchHandler> handler_strategy)
    : handler_strategy_(std::move(handler_strategy)) {}

arrow::Result<arrow::BufferVector> SerializedRecordBatchHandler::handle(
    const arrow::Buffer& source) {
  ARROW_ASSIGN_OR_RAISE(auto record_batches,
                        serialize_utils::deserializeRecordBatches(source));

  if (record_batches.empty()) {
    return arrow::BufferVector{};
  }

  ARROW_ASSIGN_OR_RAISE(auto result,
                        handler_strategy_->handle(record_batches));
  return serialize_utils::serializeRecordBatches(result);
}

}  // namespace stream_data_processor
