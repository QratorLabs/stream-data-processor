#include <arrow/compute/api.h>

#include "sort_handler.h"

#include "utils/utils.h"

namespace stream_data_processor {

arrow::Result<arrow::RecordBatchVector> SortHandler::handle(
    const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> sorted_record_batches;
  ARROW_ASSIGN_OR_RAISE(
      sorted_record_batches,
      compute_utils::groupSortingByColumns(sort_by_columns_, record_batch));

  std::shared_ptr<arrow::RecordBatch> sorted_record_batch;
  ARROW_ASSIGN_OR_RAISE(
      sorted_record_batch,
      convert_utils::concatenateRecordBatches(sorted_record_batches));

  copySchemaMetadata(*record_batch, &sorted_record_batch);
  ARROW_RETURN_NOT_OK(copyColumnTypes(*record_batch, &sorted_record_batch));
  return arrow::RecordBatchVector{sorted_record_batch};
}

}  // namespace stream_data_processor
