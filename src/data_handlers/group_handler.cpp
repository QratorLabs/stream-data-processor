#include "group_handler.h"

#include "utils/utils.h"

GroupHandler::GroupHandler(std::vector<std::string> &&grouping_columns)
    : grouping_columns_(std::forward<std::vector<std::string>>(grouping_columns)) {

}

arrow::Status GroupHandler::handle(const std::shared_ptr<arrow::Buffer> &source, std::shared_ptr<arrow::Buffer> *target) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;
  ARROW_RETURN_NOT_OK(Serializer::deserializeRecordBatches(source, &record_batches));
  if (record_batches.empty()) {
    return arrow::Status::CapacityError("No data to group");
  }

  arrow::RecordBatchVector result_record_batches;
  for (auto& record_batch : record_batches) {
    ARROW_RETURN_NOT_OK(ComputeUtils::groupSortingByColumns(grouping_columns_,
        record_batch,
        result_record_batches));
  }

  ARROW_RETURN_NOT_OK(Serializer::serializeRecordBatches(result_record_batches.back()->schema(),
      result_record_batches, target));
  return arrow::Status::OK();
}
