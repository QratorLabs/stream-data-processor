#include "pipeline_handler.h"

PipelineHandler::PipelineHandler(
    const std::vector<std::shared_ptr<RecordBatchHandler>>& pipeline_handlers)
    : pipeline_handlers_(pipeline_handlers) {}

PipelineHandler::PipelineHandler(
    std::vector<std::shared_ptr<RecordBatchHandler>>&& pipeline_handlers)
    : pipeline_handlers_(std::move(pipeline_handlers)) {}

arrow::Status PipelineHandler::handle(
    const arrow::RecordBatchVector& record_batches,
    arrow::RecordBatchVector* result) {
  arrow::RecordBatchVector current_result;
  for (auto& record_batch : record_batches) {
    current_result.push_back(arrow::RecordBatch::Make(
        record_batch->schema(), record_batch->num_rows(),
        record_batch->columns()));

    if (record_batch->schema()->HasMetadata()) {
      current_result.back() = current_result.back()->ReplaceSchemaMetadata(
          record_batch->schema()->metadata());
    }
  }

  for (auto& handler : pipeline_handlers_) {
    ARROW_RETURN_NOT_OK(handler->handle(current_result, result));

    current_result = *result;
  }

  *result = current_result;
  return arrow::Status::OK();
}
