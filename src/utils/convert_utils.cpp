#include <map>

#include <spdlog/spdlog.h>

#include "convert_utils.h"

namespace stream_data_processor {
namespace convert_utils {

arrow::Status convertTableToRecordBatch(
    const std::shared_ptr<arrow::Table>& table,
    std::shared_ptr<arrow::RecordBatch>* record_batch) {
  auto prepared_table_result = table->CombineChunks();
  if (!prepared_table_result.ok()) {
    return prepared_table_result.status();
  }

  arrow::ArrayVector table_columns;
  if (table->num_rows() != 0) {
    for (auto& column : prepared_table_result.ValueOrDie()->columns()) {
      table_columns.push_back(column->chunk(0));
    }
  }

  *record_batch = arrow::RecordBatch::Make(
      prepared_table_result.ValueOrDie()->schema(),
      prepared_table_result.ValueOrDie()->num_rows(), table_columns);
  return arrow::Status::OK();
}

arrow::Status concatenateRecordBatches(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batches,
    std::shared_ptr<arrow::RecordBatch>* target) {
  auto table_result = arrow::Table::FromRecordBatches(record_batches);
  if (!table_result.ok()) {
    return table_result.status();
  }

  ARROW_RETURN_NOT_OK(
      convertTableToRecordBatch(table_result.ValueOrDie(), target));
  return arrow::Status::OK();
}

}  // namespace convert_utils
}  // namespace stream_data_processor
