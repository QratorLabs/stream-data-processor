#include "group_handler.h"

#include "metadata/grouping.h"
#include "utils/utils.h"

namespace stream_data_processor {

arrow::Result<arrow::RecordBatchVector> GroupHandler::handle(
    const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  arrow::RecordBatchVector grouped_record_batches;

  ARROW_ASSIGN_OR_RAISE(
      grouped_record_batches,
      compute_utils::groupSortingByColumns(grouping_columns_, record_batch));

  arrow::RecordBatchVector result;
  for (auto& group : grouped_record_batches) {
    copySchemaMetadata(*record_batch, &group);

    ARROW_RETURN_NOT_OK(
        metadata::fillGroupMetadata(&group, grouping_columns_));

    ARROW_RETURN_NOT_OK(copyColumnTypes(*record_batch, &group));
    result.push_back(group);
  }

  return result;
}

}  // namespace stream_data_processor
