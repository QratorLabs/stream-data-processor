#pragma once

#include <deque>
#include <memory>

#include "period_handler.h"
#include "record_batch_handlers/record_batch_handler.h"

namespace stream_data_processor {

class SerializedPeriodHandler : public PeriodHandler {
 public:
  explicit SerializedPeriodHandler(
      std::shared_ptr<RecordBatchHandler> handler_strategy);

  [[nodiscard]] arrow::Result<arrow::BufferVector> handle(
      const std::deque<std::shared_ptr<arrow::Buffer>>& period) override;

 private:
  std::shared_ptr<RecordBatchHandler> handler_strategy_;
};

}  // namespace stream_data_processor
