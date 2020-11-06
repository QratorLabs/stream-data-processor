#pragma once

#include <memory>

#include "data_handler.h"
#include "record_batch_handlers/record_batch_handler.h"

namespace stream_data_processor {

class SerializedRecordBatchHandler : public DataHandler {
 public:
  explicit SerializedRecordBatchHandler(
      std::shared_ptr<RecordBatchHandler> handler_strategy);

  arrow::Status handle(
      const std::shared_ptr<arrow::Buffer>& source,
      std::vector<std::shared_ptr<arrow::Buffer>>* target) override;

 private:
  std::shared_ptr<RecordBatchHandler> handler_strategy_;
};

}  // namespace stream_data_processor
