#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "record_batch_handler.h"
#include "stateful_handlers/handler_factory.h"

namespace stream_data_processor {

class GroupDispatcher : public RecordBatchHandler {
 public:
  explicit GroupDispatcher(std::shared_ptr<HandlerFactory> handler_factory);

  arrow::Result<arrow::RecordBatchVector> handle(
      const std::shared_ptr<arrow::RecordBatch>& record_batch) override;

 private:
  std::shared_ptr<HandlerFactory> handler_factory_;
  std::unordered_map<std::string, std::shared_ptr<RecordBatchHandler>>
      groups_states_;
};

}  // namespace stream_data_processor
