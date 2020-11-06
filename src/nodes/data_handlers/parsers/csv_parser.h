#pragma once

#include "parser.h"

namespace stream_data_processor {

class CSVParser : public Parser {
 public:
  explicit CSVParser(std::shared_ptr<arrow::Schema> schema = nullptr);

  arrow::Status parseRecordBatches(
      const std::shared_ptr<arrow::Buffer>& buffer,
      std::vector<std::shared_ptr<arrow::RecordBatch>>* record_batches)
      override;

 private:
  arrow::Status tryFindTimeColumn();

 private:
  std::shared_ptr<arrow::Schema> record_batches_schema_;
};

}  // namespace stream_data_processor
