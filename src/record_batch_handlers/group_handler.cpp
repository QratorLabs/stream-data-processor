#include "group_handler.h"

#include "grouping/grouping.h"
#include "utils/utils.h"

arrow::Status GroupHandler::handle(
    const std::shared_ptr<arrow::RecordBatch>& record_batch,
    arrow::RecordBatchVector* result) {
  arrow::RecordBatchVector grouped_record_batches;

  ARROW_RETURN_NOT_OK(ComputeUtils::groupSortingByColumns(
      grouping_columns_, record_batch, &grouped_record_batches));

  for (auto& group : grouped_record_batches) {
    ARROW_RETURN_NOT_OK(RecordBatchGrouping::fillGroupMetadata(
        &group, grouping_columns_));
    result->push_back(group);
  }

  return arrow::Status::OK();
}
