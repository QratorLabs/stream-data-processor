#include "window_handler.h"
#include "utils/utils.h"

namespace stream_data_processor {

arrow::Result<arrow::RecordBatchVector> WindowHandler::handle(
    const arrow::RecordBatchVector& record_batches) {
  std::shared_ptr<arrow::RecordBatch> record_batch;
  ARROW_ASSIGN_OR_RAISE(
      record_batch, convert_utils::concatenateRecordBatches(record_batches));

  copySchemaMetadata(*record_batches.front(), &record_batch);
  ARROW_RETURN_NOT_OK(
      copyColumnTypes(*record_batches.front(), &record_batch));

  return arrow::RecordBatchVector{record_batch};
}

arrow::Result<arrow::RecordBatchVector> WindowHandler::handle(
    const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  return arrow::RecordBatchVector{record_batch};
}

}  // namespace stream_data_processor
