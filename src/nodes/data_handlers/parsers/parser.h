#pragma once

#include <memory>
#include <vector>

#include <arrow/api.h>

namespace stream_data_processor {

class Parser {
 public:
  virtual arrow::Status parseRecordBatches(
      const std::shared_ptr<arrow::Buffer>& buffer,
      std::vector<std::shared_ptr<arrow::RecordBatch>>* record_batches) = 0;
};

}  // namespace stream_data_processor
