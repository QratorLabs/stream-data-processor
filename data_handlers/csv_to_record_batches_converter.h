#pragma once

#include <arrow/api.h>

#include "data_handler.h"

class CSVToRecordBatchesConverter : public DataHandler {
 public:
  arrow::Status handle(std::shared_ptr<arrow::Buffer> source, std::shared_ptr<arrow::Buffer>* target) override;

 private:
  std::shared_ptr<arrow::Schema> record_batches_schema_{nullptr};
};


