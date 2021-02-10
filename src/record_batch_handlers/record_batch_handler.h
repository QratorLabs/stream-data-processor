#pragma once

#include <memory>
#include <vector>

#include <arrow/api.h>

#include "metadata/column_typing.h"
#include "utils/convert_utils.h"

namespace stream_data_processor {

class RecordBatchHandler {
 public:
  virtual arrow::Result<arrow::RecordBatchVector> handle(
      const std::shared_ptr<arrow::RecordBatch>& record_batch) = 0;

  virtual arrow::Result<arrow::RecordBatchVector> handle(
      const arrow::RecordBatchVector& record_batches) {
    arrow::RecordBatchVector result;
    for (auto& record_batch : record_batches) {
      ARROW_ASSIGN_OR_RAISE(auto batch_result, handle(record_batch));
      convert_utils::append(std::move(batch_result), result);
    }

    return result;
  }

  virtual ~RecordBatchHandler() = 0;

 protected:
  RecordBatchHandler() = default;

  RecordBatchHandler(const RecordBatchHandler& /* non-used */) = default;
  RecordBatchHandler& operator=(const RecordBatchHandler& /* non-used */) =
      default;

  RecordBatchHandler(RecordBatchHandler&& /* non-used */) = default;
  RecordBatchHandler& operator=(RecordBatchHandler&& /* non-used */) =
      default;

  static void copySchemaMetadata(const arrow::RecordBatch& from,
                                 std::shared_ptr<arrow::RecordBatch>* to);

  static arrow::Status copyColumnTypes(
      const arrow::RecordBatch& from,
      std::shared_ptr<arrow::RecordBatch>* to);
};

}  // namespace stream_data_processor
