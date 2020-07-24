#include <memory>
#include <utility>
#include <vector>

#include <arrow/csv/api.h>
#include <arrow/io/api.h>

#include "data_parser.h"
#include "utils/serializer.h"

DataParser::DataParser(std::shared_ptr<Parser> parser) : parser_(std::move(parser)) {

}

arrow::Status DataParser::handle(std::shared_ptr<arrow::Buffer> source,
                                 std::shared_ptr<arrow::Buffer> *target) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;
  bool first_batch = record_batches_schema_ == nullptr;
  ARROW_RETURN_NOT_OK(parser_->parseRecordBatches(source, &record_batches, first_batch));
  if (first_batch) {
    if (record_batches.empty()) {
      return arrow::Status::CapacityError("No data to handle");
    } else {
      record_batches_schema_ = record_batches.front()->schema();
    }
  } else {
    for (auto& record_batch : record_batches) {
      record_batch = arrow::RecordBatch::Make(record_batches_schema_,
          record_batch->num_rows(),
          record_batch->columns());
    }
  }

  ARROW_RETURN_NOT_OK(Serializer::serializeRecordBatches(record_batches_schema_, record_batches, target));
  return arrow::Status::OK();
}
