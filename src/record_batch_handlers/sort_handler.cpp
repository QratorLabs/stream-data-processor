#include <arrow/compute/api.h>

#include "sort_handler.h"

#include "utils/utils.h"

arrow::Status SortHandler::handle(
    const arrow::RecordBatchVector& record_batches,
    arrow::RecordBatchVector* result) {
  for (auto& record_batch : record_batches) {
    std::vector<std::shared_ptr<arrow::RecordBatch>> sorted_record_batches;
    ARROW_RETURN_NOT_OK(ComputeUtils::groupSortingByColumns(
        sort_by_columns_, record_batch, &sorted_record_batches));

    std::shared_ptr<arrow::RecordBatch> sorted_record_batch;
    ARROW_RETURN_NOT_OK(DataConverter::concatenateRecordBatches(
        sorted_record_batches, &sorted_record_batch));

    if (record_batch->schema()->HasMetadata()) {
      sorted_record_batch = sorted_record_batch->ReplaceSchemaMetadata(
          record_batch->schema()->metadata());
    }

    result->push_back(sorted_record_batch);
  }

  return arrow::Status::OK();
}
