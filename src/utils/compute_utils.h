#pragma once

#include <memory>
#include <vector>

#include <arrow/api.h>

namespace stream_data_processor {
namespace compute_utils {

arrow::Result<arrow::RecordBatchVector> groupSortingByColumns(
    const std::vector<std::string>& column_names,
    const std::shared_ptr<arrow::RecordBatch>& record_batch);

arrow::Result<std::shared_ptr<arrow::RecordBatch>> sortByColumn(
    const std::string& column_name,
    const std::shared_ptr<arrow::RecordBatch>& source);

arrow::Result<std::pair<size_t, size_t>> argMinMax(
    std::shared_ptr<arrow::Array> array);

}  // namespace compute_utils
}  // namespace stream_data_processor
