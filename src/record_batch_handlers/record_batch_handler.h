#pragma once

#include <memory>
#include <vector>

#include <arrow/api.h>

#include "metadata/column_typing.h"

namespace stream_data_processor {

class RecordBatchHandler {
 public:
  virtual arrow::Status handle(
      const std::shared_ptr<arrow::RecordBatch>& record_batch,
      arrow::RecordBatchVector* result) = 0;

  virtual arrow::Status handle(const arrow::RecordBatchVector& record_batches,
                               arrow::RecordBatchVector* result) {
    for (auto& record_batch : record_batches) {
      ARROW_RETURN_NOT_OK(handle(record_batch, result));
    }

    return arrow::Status::OK();
  }

 protected:
  static void copySchemaMetadata(const arrow::RecordBatch& from,
                                 std::shared_ptr<arrow::RecordBatch>* to) {
    if (from.schema()->HasMetadata()) {
      *to = to->get()->ReplaceSchemaMetadata(from.schema()->metadata());
    }
  }

  static arrow::Status copyColumnTypes(
      const arrow::RecordBatch& from,
      std::shared_ptr<arrow::RecordBatch>* to) {
    for (auto& from_field : from.schema()->fields()) {
      auto to_field = to->get()->schema()->GetFieldByName(from_field->name());
      if (to_field == nullptr) {
        continue;
      }

      if (to_field->Equals(from_field)) {
        ARROW_RETURN_NOT_OK(metadata::setColumnTypeMetadata(
            &to_field, metadata::getColumnType(*from_field)));
        auto set_field_result = to->get()->schema()->SetField(
            to->get()->schema()->GetFieldIndex(from_field->name()), to_field);
        ARROW_RETURN_NOT_OK(set_field_result.status());
      }
    }

    return arrow::Status::OK();
  }
};

}  // namespace stream_data_processor
