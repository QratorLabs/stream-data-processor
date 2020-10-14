#include "window_handler.h"
#include "utils/utils.h"

arrow::Status WindowHandler::handle(
    const arrow::RecordBatchVector& record_batches,
    arrow::RecordBatchVector* result) {
  std::shared_ptr<arrow::RecordBatch> record_batch;
  ARROW_RETURN_NOT_OK(
      DataConverter::concatenateRecordBatches(record_batches, &record_batch));

  result->push_back(record_batch);
  return arrow::Status::OK();
}
