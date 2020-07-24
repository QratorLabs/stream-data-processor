#pragma once

#include <arrow/api.h>

#include "data_handler.h"
#include "utils/parsers/parser.h"

class DataParser : public DataHandler {
 public:
  explicit DataParser(std::shared_ptr<Parser> parser);

  arrow::Status handle(std::shared_ptr<arrow::Buffer> source, std::shared_ptr<arrow::Buffer>* target) override;

 private:
  std::shared_ptr<Parser> parser_;
  std::shared_ptr<arrow::Schema> record_batches_schema_{nullptr};
};


