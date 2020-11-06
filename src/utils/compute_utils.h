#pragma once

#include <memory>
#include <vector>

#include <arrow/api.h>

namespace stream_data_processor {
namespace compute_utils {

arrow::Status groupSortingByColumns(
    const std::vector<std::string>& column_names,
    const std::shared_ptr<arrow::RecordBatch>& record_batch,
    std::vector<std::shared_ptr<arrow::RecordBatch>>* grouped);

arrow::Status sortByColumn(const std::string& column_name,
                           const std::shared_ptr<arrow::RecordBatch>& source,
                           std::shared_ptr<arrow::RecordBatch>* target);

arrow::Status argMinMax(std::shared_ptr<arrow::Array> array,
                        std::pair<size_t, size_t>* arg_min_max);

}  // namespace compute_utils
}  // namespace stream_data_processor
