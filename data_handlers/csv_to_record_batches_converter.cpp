#include <memory>
#include <vector>

#include <arrow/csv/api.h>
#include <arrow/io/api.h>

#include "csv_to_record_batches_converter.h"

#include "utils.h"

arrow::Status CSVToRecordBatchesConverter::handle(std::shared_ptr<arrow::Buffer> source,
                                                  std::shared_ptr<arrow::Buffer> *target) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;
  ARROW_RETURN_NOT_OK(Utils::parseCSVToRecordBatches(source, &record_batches));
  if (record_batches_schema_ == nullptr) {
    if (record_batches.empty()) {
      return arrow::Status::CapacityError("No data to handle");
    } else {
      record_batches_schema_ = record_batches.front()->schema();
    }
  }

  ARROW_RETURN_NOT_OK(Utils::serializeRecordBatches(*record_batches_schema_, record_batches, target));
  return arrow::Status::OK();
}
