#pragma once

#include <arrow/api.h>

#include "data_handler.h"
#include "parsers/parser.h"

namespace stream_data_processor {

class DataParser : public DataHandler {
 public:
  explicit DataParser(std::shared_ptr<Parser> parser);

  arrow::Status handle(
      const arrow::Buffer& source,
      std::vector<std::shared_ptr<arrow::Buffer>>* target) override;

 private:
  std::shared_ptr<Parser> parser_;
};

}  // namespace stream_data_processor
