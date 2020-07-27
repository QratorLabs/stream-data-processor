#pragma once

#include "parser.h"

class CSVParser : public Parser {
 public:
  explicit CSVParser(std::shared_ptr<arrow::Schema> schema = nullptr);

  arrow::Status parseRecordBatches(const std::shared_ptr<arrow::Buffer>& buffer,
                                   std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batches) override;

 private:
  std::shared_ptr<arrow::Schema> record_batches_schema_;
};


