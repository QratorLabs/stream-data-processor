#include <arrow/compute/api.h>

#include "sort_handler.h"

#include "utils/utils.h"

arrow::Status SortHandler::handle(const std::shared_ptr<arrow::Buffer> &source, std::shared_ptr<arrow::Buffer> *target) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;
  ARROW_RETURN_NOT_OK(Serializer::deserializeRecordBatches(source, &record_batches));
  if (record_batches.empty()) {
    return arrow::Status::CapacityError("No data to sort");
  }

  std::shared_ptr<arrow::RecordBatch> record_batch;
  ARROW_RETURN_NOT_OK(DataConverter::concatenateRecordBatches(record_batches, &record_batch));

  std::vector<std::shared_ptr<arrow::RecordBatch>> sorted_record_batches;
  ARROW_RETURN_NOT_OK(ComputeUtils::groupSortingByColumns(column_names_, record_batch, sorted_record_batches));

  ARROW_RETURN_NOT_OK(DataConverter::concatenateRecordBatches(sorted_record_batches, &record_batch));

  ARROW_RETURN_NOT_OK(Serializer::serializeRecordBatches(record_batch->schema(), { record_batch }, target));
  return arrow::Status::OK();
}
