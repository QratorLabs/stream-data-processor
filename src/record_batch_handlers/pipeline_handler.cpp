#include "pipeline_handler.h"

namespace stream_data_processor {

PipelineHandler::PipelineHandler(
    const std::vector<std::shared_ptr<RecordBatchHandler>>& pipeline_handlers)
    : pipeline_handlers_(pipeline_handlers) {}

PipelineHandler::PipelineHandler(
    std::vector<std::shared_ptr<RecordBatchHandler>>&& pipeline_handlers)
    : pipeline_handlers_(std::move(pipeline_handlers)) {}

arrow::Result<arrow::RecordBatchVector> PipelineHandler::handle(
    const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  return handle(arrow::RecordBatchVector{record_batch});
}

arrow::Result<arrow::RecordBatchVector> PipelineHandler::handle(
    const arrow::RecordBatchVector& record_batches) {
  arrow::RecordBatchVector current_result(record_batches);

  for (auto& handler : pipeline_handlers_) {
    ARROW_ASSIGN_OR_RAISE(auto tmp_result, handler->handle(current_result));
    current_result = std::move(tmp_result);
  }

  return current_result;
}

}  // namespace stream_data_processor
