#pragma once

#include <memory>

#include <arrow/api.h>

class ArrowUtils {
 public:
  static arrow::Status makeArrayBuilder(arrow::Type::type type,
                                        std::shared_ptr<arrow::ArrayBuilder>* builder,
                                        arrow::MemoryPool* pool = arrow::default_memory_pool());

  static arrow::Status appendToBuilder(const std::shared_ptr<arrow::Scalar>& value,
                                       std::shared_ptr<arrow::ArrayBuilder>* builder,
                                       arrow::Type::type type);
};
