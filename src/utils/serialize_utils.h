#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

namespace stream_data_processor {
namespace serialize_utils {

arrow::Result<arrow::BufferVector> serializeRecordBatches(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batches);

arrow::Result<arrow::RecordBatchVector> deserializeRecordBatches(
    const arrow::Buffer& buffer);

}  // namespace serialize_utils
}  // namespace stream_data_processor
