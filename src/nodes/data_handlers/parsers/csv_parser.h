#pragma once

#include "parser.h"

namespace stream_data_processor {

class CSVParser : public Parser {
 public:
  explicit CSVParser(std::shared_ptr<arrow::Schema> schema = nullptr);

  [[nodiscard]] arrow::Result<arrow::RecordBatchVector> parseRecordBatches(
      const arrow::Buffer& buffer) override;

 private:
  arrow::Status tryFindTimeColumn();

 private:
  std::shared_ptr<arrow::Schema> record_batches_schema_;
};

}  // namespace stream_data_processor
