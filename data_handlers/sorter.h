#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "data_handler.h"

class Sorter : public DataHandler {
 public:
  explicit Sorter(std::vector<std::string> column_names);

  arrow::Status handle(std::shared_ptr<arrow::Buffer> source, std::shared_ptr<arrow::Buffer>* target) override;

 private:
  arrow::Status sortByColumn(size_t i, const std::shared_ptr<arrow::RecordBatch>& source, std::shared_ptr<arrow::RecordBatch>* target) const;
  arrow::Status sort(size_t i, const std::shared_ptr<arrow::RecordBatch>& source, std::vector<std::shared_ptr<arrow::RecordBatch>> &targets) const;

 private:
  std::vector<std::string> column_names_;
};


