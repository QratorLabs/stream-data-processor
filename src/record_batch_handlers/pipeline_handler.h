#pragma once

#include <vector>

#include <arrow/api.h>

#include "record_batch_handler.h"

class PipelineHandler : public RecordBatchHandler {
 public:
  explicit PipelineHandler(const std::vector<std::shared_ptr<RecordBatchHandler>>& pipeline_handlers = { });
  explicit PipelineHandler(std::vector<std::shared_ptr<RecordBatchHandler>>&& pipeline_handlers);

  arrow::Status handle(const arrow::RecordBatchVector& record_batches, arrow::RecordBatchVector* result) override;

  template <typename HandlerType>
  void pushBackHandler(HandlerType&& handler) {
    pipeline_handlers_.push_back(std::forward<HandlerType>(handler));
  }

  void popBackHandler() {
    pipeline_handlers_.pop_back();
  }

 private:
  std::vector<std::shared_ptr<RecordBatchHandler>> pipeline_handlers_;
};
