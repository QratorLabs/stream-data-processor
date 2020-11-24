#pragma once

#include <vector>

#include <arrow/api.h>

#include "record_batch_handler.h"

namespace stream_data_processor {

class PipelineHandler : public RecordBatchHandler {
 public:
  explicit PipelineHandler(
      const std::vector<std::shared_ptr<RecordBatchHandler>>&
          pipeline_handlers = {});
  explicit PipelineHandler(
      std::vector<std::shared_ptr<RecordBatchHandler>>&& pipeline_handlers);

  arrow::Result<arrow::RecordBatchVector> handle(
      const std::shared_ptr<arrow::RecordBatch>& record_batch) override;

  arrow::Result<arrow::RecordBatchVector> handle(
      const arrow::RecordBatchVector& record_batches) override;

  template <typename HandlerType>
  void pushBackHandler(HandlerType&& handler) {
    pipeline_handlers_.push_back(std::forward<HandlerType>(handler));
  }

  void popBackHandler() { pipeline_handlers_.pop_back(); }

 private:
  std::vector<std::shared_ptr<RecordBatchHandler>> pipeline_handlers_;
};

}  // namespace stream_data_processor
