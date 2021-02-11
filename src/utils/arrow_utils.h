#pragma once

#include <memory>

#include <arrow/api.h>

namespace stream_data_processor {
namespace arrow_utils {

arrow::Result<std::shared_ptr<arrow::ArrayBuilder>> createArrayBuilder(
    arrow::Type::type type,
    arrow::MemoryPool* pool = arrow::default_memory_pool());

arrow::Result<std::shared_ptr<arrow::ArrayBuilder>>
createTimestampArrayBuilder(
    const std::shared_ptr<arrow::DataType>& arrow_type,
    arrow::MemoryPool* pool);

arrow::Status appendToBuilder(const std::shared_ptr<arrow::Scalar>& value,
                              std::shared_ptr<arrow::ArrayBuilder>* builder,
                              arrow::Type::type type);

arrow::Status appendToTimestampBuilder(
    const std::shared_ptr<arrow::Scalar>& value,
    std::shared_ptr<arrow::ArrayBuilder>* builder,
    const std::shared_ptr<arrow::DataType>& arrow_type);

bool isNumericType(arrow::Type::type type);

arrow::Result<std::shared_ptr<arrow::Scalar>> castTimestampScalar(
    const arrow::Result<std::shared_ptr<arrow::Scalar>>& timestamp_scalar,
    arrow::TimeUnit::type time_unit);

}  // namespace arrow_utils
}  // namespace stream_data_processor
