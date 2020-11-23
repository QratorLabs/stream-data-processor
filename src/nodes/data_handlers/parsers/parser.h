#pragma once

#include <memory>
#include <vector>

#include <arrow/api.h>

namespace stream_data_processor {

class Parser {
 public:
  [[nodiscard]] virtual arrow::Result<arrow::RecordBatchVector>
  parseRecordBatches(const arrow::Buffer& buffer) = 0;
};

}  // namespace stream_data_processor
