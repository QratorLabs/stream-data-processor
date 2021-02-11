#pragma once

#include <memory>

#include <arrow/api.h>

#include "utils/time_utils.h"

namespace stream_data_processor {
namespace metadata {

arrow::Status setTimeUnitMetadata(std::shared_ptr<arrow::Field>* field,
                                  time_utils::TimeUnit time_unit);

arrow::Status setTimeUnitMetadata(
    std::shared_ptr<arrow::RecordBatch>* record_batch,
    const std::string& column_name, time_utils::TimeUnit time_unit);

arrow::Result<time_utils::TimeUnit> getTimeUnitMetadata(
    const arrow::Field& field);

arrow::Result<time_utils::TimeUnit> getTimeUnitMetadata(
    const arrow::RecordBatch& record_batch, const std::string& column_name);

}  // namespace metadata
}  // namespace stream_data_processor
