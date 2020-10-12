#include "group_handler.h"

#include "utils/utils.h"

arrow::Status GroupHandler::handle(
    const arrow::RecordBatchVector& record_batches,
    arrow::RecordBatchVector* result) {
  for (auto& record_batch : record_batches) {
    ARROW_RETURN_NOT_OK(ComputeUtils::groupSortingByColumns(
        grouping_columns_, record_batch, result));
  }
  return arrow::Status::OK();
}
