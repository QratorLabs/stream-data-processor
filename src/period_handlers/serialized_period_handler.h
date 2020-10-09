#pragma once

#include <deque>
#include <memory>

#include "period_handler.h"
#include "record_batch_handlers/record_batch_handler.h"

class SerializedPeriodHandler : public PeriodHandler {
 public:
  explicit SerializedPeriodHandler(std::shared_ptr<RecordBatchHandler> handler_strategy);

  arrow::Status handle(const std::deque<std::shared_ptr<arrow::Buffer>>& period,
                       std::shared_ptr<arrow::Buffer>* target) override;

 private:
  std::shared_ptr<RecordBatchHandler> handler_strategy_;
};
