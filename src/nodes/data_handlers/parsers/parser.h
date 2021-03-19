#pragma once

#include <memory>
#include <vector>

#include <arrow/api.h>

namespace stream_data_processor {

class Parser {
 public:
  [[nodiscard]] virtual arrow::Result<arrow::RecordBatchVector>
  parseRecordBatches(const arrow::Buffer& buffer) = 0;

  virtual ~Parser() = 0;

 protected:
  Parser() = default;

  Parser(const Parser& /* non-used */) = default;
  Parser& operator=(const Parser& /* non-used */) = default;

  Parser(Parser&& /* non-used */) = default;
  Parser& operator=(Parser&& /* non-used */) = default;
};

}  // namespace stream_data_processor
