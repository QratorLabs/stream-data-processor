#include "window_handler.h"
#include "utils/utils.h"

arrow::Status WindowHandler::handle(const std::deque<std::shared_ptr<arrow::Buffer>> &period,
                                    std::shared_ptr<arrow::Buffer> &result) {
  arrow::RecordBatchVector period_vector;
  for (auto& buffer : period) {
    arrow::RecordBatchVector record_batch_vector;
    ARROW_RETURN_NOT_OK(Serializer::deserializeRecordBatches(buffer, &record_batch_vector));
    for (auto& record_batch : record_batch_vector) {
      period_vector.push_back(record_batch);
    }
  }

  std::shared_ptr<arrow::RecordBatch> record_batch;
  ARROW_RETURN_NOT_OK(DataConverter::concatenateRecordBatches(period_vector, &record_batch));
  ARROW_RETURN_NOT_OK(Serializer::serializeRecordBatches(record_batch->schema(), {record_batch}, &result));
  return arrow::Status::OK();
}
