#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "record_batch_handler.h"
#include "stateful_handlers/stateful_handler.h"

class GroupDispatcher : public RecordBatchHandler {
 public:
  explicit GroupDispatcher(std::shared_ptr<StatefulHandler> initial_state);

  arrow::Status handle(
      const std::shared_ptr<arrow::RecordBatch>& record_batch,
      arrow::RecordBatchVector* result) override;

 private:
  std::shared_ptr<const StatefulHandler> initial_state_;
  std::unordered_map<std::string, std::shared_ptr<StatefulHandler> >
      groups_states_;
};
