#include <map>

#include <spdlog/spdlog.h>

#include "convert_utils.h"

namespace stream_data_processor {
namespace convert_utils {

arrow::Result<std::shared_ptr<arrow::RecordBatch>> convertTableToRecordBatch(
    const arrow::Table& table) {
  std::shared_ptr<arrow::Table> prepared_table;
  ARROW_ASSIGN_OR_RAISE(prepared_table, table.CombineChunks());
  arrow::ArrayVector table_columns;
  if (table.num_rows() != 0) {
    for (auto& column : prepared_table->columns()) {
      table_columns.push_back(column->chunk(0));
    }
  }

  return arrow::RecordBatch::Make(prepared_table->schema(),
                                  prepared_table->num_rows(), table_columns);
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> concatenateRecordBatches(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batches) {
  std::shared_ptr<arrow::Table> table;
  ARROW_ASSIGN_OR_RAISE(table,
                        arrow::Table::FromRecordBatches(record_batches));

  return convertTableToRecordBatch(*table);
}

}  // namespace convert_utils
}  // namespace stream_data_processor
