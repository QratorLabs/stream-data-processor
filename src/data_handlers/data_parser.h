#pragma once

#include <arrow/api.h>

#include "data_handler.h"
#include "utils/parsers/parser.h"

class DataParser : public DataHandler {
 public:
  explicit DataParser(std::shared_ptr<Parser> parser);

  arrow::Status handle(const std::shared_ptr<arrow::Buffer> &source, std::shared_ptr<arrow::Buffer>* target) override;

 private:
  std::shared_ptr<Parser> parser_;
};
