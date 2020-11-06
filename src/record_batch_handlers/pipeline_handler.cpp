#include "pipeline_handler.h"

namespace stream_data_processor {

PipelineHandler::PipelineHandler(
    const std::vector<std::shared_ptr<RecordBatchHandler>>& pipeline_handlers)
    : pipeline_handlers_(pipeline_handlers) {}

PipelineHandler::PipelineHandler(
    std::vector<std::shared_ptr<RecordBatchHandler>>&& pipeline_handlers)
    : pipeline_handlers_(std::move(pipeline_handlers)) {}

arrow::Status PipelineHandler::handle(
    const std::shared_ptr<arrow::RecordBatch>& record_batch,
    arrow::RecordBatchVector* result) {
  ARROW_RETURN_NOT_OK(handle({record_batch}, result));
  return arrow::Status::OK();
}

arrow::Status PipelineHandler::handle(
    const arrow::RecordBatchVector& record_batches,
    arrow::RecordBatchVector* result) {
  arrow::RecordBatchVector current_result(record_batches);

  for (auto& handler : pipeline_handlers_) {
    arrow::RecordBatchVector tmp_result;
    ARROW_RETURN_NOT_OK(handler->handle(current_result, &tmp_result));
    current_result = std::move(tmp_result);
  }

  *result = std::move(current_result);
  return arrow::Status::OK();
}

}  // namespace stream_data_processor
