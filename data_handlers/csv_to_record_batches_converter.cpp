#include <memory>
#include <vector>

#include <arrow/csv/api.h>
#include <arrow/io/api.h>

#include "csv_to_record_batches_converter.h"
#include "utils.h"

CSVToRecordBatchesConverter::CSVToRecordBatchesConverter(bool partial_input)
    : partial_input_(partial_input)
    , in_progress_(false) {

}

arrow::Status CSVToRecordBatchesConverter::handle(std::shared_ptr<arrow::Buffer> source,
                                                  std::shared_ptr<arrow::Buffer> *target) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;
  ARROW_RETURN_NOT_OK(Utils::parseCSVToRecordBatches(source, &record_batches));
  ARROW_RETURN_NOT_OK(Utils::serializeRecordBatches(record_batches, target, !in_progress_));
  if (partial_input_) {
    in_progress_ = true;
  }

  return arrow::Status::OK();
}

void CSVToRecordBatchesConverter::reset() {
  if (partial_input_) {
    in_progress_ = false;
  }
}
