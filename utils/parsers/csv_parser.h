#pragma once

#include "parser.h"

class CSVParser : public Parser {
 public:
  arrow::Status parseRecordBatches(std::shared_ptr<arrow::Buffer> buffer,
                                   std::vector<std::shared_ptr<arrow::RecordBatch>>* record_batches,
                                   bool read_column_names = false) override;
};


