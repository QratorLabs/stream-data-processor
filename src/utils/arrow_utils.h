#pragma once

#include <memory>

#include <arrow/api.h>

namespace stream_data_processor {
namespace arrow_utils {

arrow::Status makeArrayBuilder(
    arrow::Type::type type, std::shared_ptr<arrow::ArrayBuilder>* builder,
    arrow::MemoryPool* pool = arrow::default_memory_pool());

arrow::Status appendToBuilder(const std::shared_ptr<arrow::Scalar>& value,
                              std::shared_ptr<arrow::ArrayBuilder>* builder,
                              arrow::Type::type type);

}  // namespace arrow_utils
}  // namespace stream_data_processor
