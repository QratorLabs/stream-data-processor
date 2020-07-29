#include <memory>
#include <utility>
#include <vector>

#include <arrow/csv/api.h>
#include <arrow/io/api.h>

#include "data_parser.h"
#include "utils/serializer.h"

DataParser::DataParser(std::shared_ptr<Parser> parser) : parser_(std::move(parser)) {

}

arrow::Status DataParser::handle(const std::shared_ptr<arrow::Buffer> &source,
                                 std::shared_ptr<arrow::Buffer> *target) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;
  ARROW_RETURN_NOT_OK(parser_->parseRecordBatches(source, record_batches));
  if (record_batches.empty()) {
    return arrow::Status::CapacityError("No data to parse");
  }

  ARROW_RETURN_NOT_OK(Serializer::serializeRecordBatches(record_batches.back()->schema(), record_batches, target));
  return arrow::Status::OK();
}
