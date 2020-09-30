#include <arrow/compute/api.h>

#include "sort_handler.h"

#include "utils/utils.h"

arrow::Status SortHandler::handle(const arrow::RecordBatchVector& record_batches, arrow::RecordBatchVector* result) {
  std::shared_ptr<arrow::RecordBatch> record_batch;
  ARROW_RETURN_NOT_OK(DataConverter::concatenateRecordBatches(record_batches, &record_batch));

  std::vector<std::shared_ptr<arrow::RecordBatch>> sorted_record_batches;
  ARROW_RETURN_NOT_OK(ComputeUtils::groupSortingByColumns(column_names_, record_batch, &sorted_record_batches));

  ARROW_RETURN_NOT_OK(DataConverter::concatenateRecordBatches(sorted_record_batches, &record_batch));

  result->push_back(record_batch);
  return arrow::Status::OK();
}
