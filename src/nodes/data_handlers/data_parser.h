#pragma once

#include <arrow/api.h>

#include "data_handler.h"
#include "parsers/parser.h"

namespace stream_data_processor {

class DataParser : public DataHandler {
 public:
  explicit DataParser(std::shared_ptr<Parser> parser);

  [[nodiscard]] arrow::Result<arrow::BufferVector> handle(
      const arrow::Buffer& source) override;

 private:
  std::shared_ptr<Parser> parser_;
};

}  // namespace stream_data_processor
