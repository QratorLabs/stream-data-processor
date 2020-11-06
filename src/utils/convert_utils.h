#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include <arrow/api.h>

namespace stream_data_processor {
namespace convert_utils {

arrow::Status convertTableToRecordBatch(
    const std::shared_ptr<arrow::Table>& table,
    std::shared_ptr<arrow::RecordBatch>* record_batch);

arrow::Status concatenateRecordBatches(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batches,
    std::shared_ptr<arrow::RecordBatch>* target);

}  // namespace convert_utils
}  // namespace stream_data_processor
