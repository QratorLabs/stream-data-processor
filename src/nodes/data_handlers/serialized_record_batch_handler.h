#pragma once

#include <memory>

#include "data_handler.h"
#include "record_batch_handlers/record_batch_handler.h"

namespace stream_data_processor {

class SerializedRecordBatchHandler : public DataHandler {
 public:
  explicit SerializedRecordBatchHandler(
      std::shared_ptr<RecordBatchHandler> handler_strategy);

  [[nodiscard]] arrow::Result<arrow::BufferVector> handle(
      const arrow::Buffer& source) override;

 private:
  std::shared_ptr<RecordBatchHandler> handler_strategy_;
};

}  // namespace stream_data_processor
