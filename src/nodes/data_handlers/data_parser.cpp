#include <memory>
#include <utility>

#include <arrow/csv/api.h>
#include <arrow/io/api.h>

#include "data_parser.h"
#include "utils/serialize_utils.h"

namespace stream_data_processor {

DataParser::DataParser(std::shared_ptr<Parser> parser)
    : parser_(std::move(parser)) {}

arrow::Result<arrow::BufferVector> DataParser::handle(
    const arrow::Buffer& source) {
  ARROW_ASSIGN_OR_RAISE(auto record_batches,
                        parser_->parseRecordBatches(source));
  if (record_batches.empty()) {
    return arrow::BufferVector{};
  }

  return serialize_utils::serializeRecordBatches(record_batches);
}

}  // namespace stream_data_processor
