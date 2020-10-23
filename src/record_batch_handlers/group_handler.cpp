#include "group_handler.h"

#include "grouping/grouping.h"
#include "utils/utils.h"

arrow::Status GroupHandler::handle(
    const arrow::RecordBatchVector& record_batches,
    arrow::RecordBatchVector* result) {
  for (auto& record_batch : record_batches) {
    ARROW_RETURN_NOT_OK(ComputeUtils::groupSortingByColumns(
        grouping_columns_, record_batch, result));
  }

  for (auto& record_batch : *result) {
    ARROW_RETURN_NOT_OK(RecordBatchGrouping::fillGroupMetadata(
        &record_batch, grouping_columns_));
  }

  return arrow::Status::OK();
}
