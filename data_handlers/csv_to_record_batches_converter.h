#pragma once

#include <arrow/api.h>

#include "data_handler.h"

class CSVToRecordBatchesConverter : public DataHandler {
 public:
  explicit CSVToRecordBatchesConverter(bool partial_input = true);

  arrow::Status handle(std::shared_ptr<arrow::Buffer> source, std::shared_ptr<arrow::Buffer>* target) override;

  void reset() override;

 private:
  bool partial_input_;
  bool in_progress_;
};


