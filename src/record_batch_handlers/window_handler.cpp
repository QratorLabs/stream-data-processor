#include "window_handler.h"
#include "utils/utils.h"

namespace stream_data_processor {

arrow::Status WindowHandler::handle(
    const arrow::RecordBatchVector& record_batches,
    arrow::RecordBatchVector* result) {
  std::shared_ptr<arrow::RecordBatch> record_batch;
  ARROW_RETURN_NOT_OK(
      convert_utils::concatenateRecordBatches(record_batches, &record_batch));

  copySchemaMetadata(*record_batches.front(), &record_batch);
  ARROW_RETURN_NOT_OK(
      copyColumnTypes(*record_batches.front(), &record_batch));
  result->push_back(record_batch);

  return arrow::Status::OK();
}

arrow::Status WindowHandler::handle(
    const std::shared_ptr<arrow::RecordBatch>& record_batch,
    arrow::RecordBatchVector* result) {
  result->push_back(record_batch);
  return arrow::Status::OK();
}

}  // namespace stream_data_processor
