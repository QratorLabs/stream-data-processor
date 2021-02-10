#include "arrow_utils.h"

namespace stream_data_processor {
namespace arrow_utils {

arrow::Result<std::shared_ptr<arrow::ArrayBuilder>> createArrayBuilder(
    arrow::Type::type type, arrow::MemoryPool* pool) {
  switch (type) {
    case arrow::Type::INT64:
      return std::make_shared<arrow::Int64Builder>(pool);
    case arrow::Type::DOUBLE:
      return std::make_shared<arrow::DoubleBuilder>(pool);
    case arrow::Type::STRING:
      return std::make_shared<arrow::StringBuilder>(pool);
    case arrow::Type::BOOL:
      return std::make_shared<arrow::BooleanBuilder>(pool);
    default:
      return arrow::Status::NotImplemented(
          "Step-by-step array building currently supports one of "
          "{arrow::int64, arrow::float64, arrow::utf8, arrow::boolean, "
          "types fields "
          "only");  // TODO: support any type
  }
}

arrow::Result<std::shared_ptr<arrow::ArrayBuilder>>
createTimestampArrayBuilder(
    const std::shared_ptr<arrow::DataType>& arrow_type,
    arrow::MemoryPool* pool) {
  if (arrow_type->id() != arrow::Type::TIMESTAMP) {
    return arrow::Status::TypeError(
        "arrow::Type::TIMESTAMP type is required "
        "to create timestamp ArrayBuilder");
  }

  return std::make_shared<arrow::TimestampBuilder>(arrow_type, pool);
}

arrow::Status appendToBuilder(const std::shared_ptr<arrow::Scalar>& value,
                              std::shared_ptr<arrow::ArrayBuilder>* builder,
                              arrow::Type::type type) {
  switch (type) {
    case arrow::Type::INT64:
      ARROW_RETURN_NOT_OK(
          std::static_pointer_cast<arrow::Int64Builder>(*builder)->Append(
              std::static_pointer_cast<arrow::Int64Scalar>(value)->value));
      return arrow::Status::OK();
    case arrow::Type::DOUBLE:
      ARROW_RETURN_NOT_OK(
          std::static_pointer_cast<arrow::DoubleBuilder>(*builder)->Append(
              std::static_pointer_cast<arrow::DoubleScalar>(value)->value));
      return arrow::Status::OK();
    case arrow::Type::STRING:
      ARROW_RETURN_NOT_OK(
          std::static_pointer_cast<arrow::StringBuilder>(*builder)->Append(
              std::static_pointer_cast<arrow::StringScalar>(value)
                  ->value->ToString()));
      return arrow::Status::OK();
    case arrow::Type::BOOL:
      ARROW_RETURN_NOT_OK(
          std::static_pointer_cast<arrow::BooleanBuilder>(*builder)->Append(
              std::static_pointer_cast<arrow::BooleanScalar>(value)->value));
      return arrow::Status::OK();
    default:
      return arrow::Status::NotImplemented(
          "Expected one of {arrow::int64, arrow::float64, arrow::utf8, "
          "arrow::boolean "
          "types");  // TODO: support any type
  }
}

arrow::Status appendToTimestampBuilder(
    const std::shared_ptr<arrow::Scalar>& value,
    std::shared_ptr<arrow::ArrayBuilder>* builder,
    const std::shared_ptr<arrow::DataType>& arrow_type) {
  if (arrow_type->id() != arrow::Type::TIMESTAMP) {
    return arrow::Status::TypeError(
        "arrow::Type::TIMESTAMP type is required "
        "to append value to timestamp "
        "ArrayBuilder");
  }

  ARROW_ASSIGN_OR_RAISE(auto timestamp_scalar, value->CastTo(arrow_type));

  ARROW_RETURN_NOT_OK(
      std::static_pointer_cast<arrow::TimestampBuilder>(*builder)->Append(
          std::static_pointer_cast<arrow::TimestampScalar>(timestamp_scalar)
              ->value));

  return arrow::Status::OK();
}

bool isNumericType(arrow::Type::type type) {
  switch (type) {
    case arrow::Type::UINT8:
    case arrow::Type::INT8:
    case arrow::Type::UINT16:
    case arrow::Type::INT16:
    case arrow::Type::UINT32:
    case arrow::Type::INT32:
    case arrow::Type::UINT64:
    case arrow::Type::INT64:
    case arrow::Type::HALF_FLOAT:
    case arrow::Type::FLOAT:
    case arrow::Type::DOUBLE:
    case arrow::Type::DECIMAL: return true;
    default: return false;
  }
}

arrow::Result<std::shared_ptr<arrow::Scalar>> castTimestampScalar(
    const arrow::Result<std::shared_ptr<arrow::Scalar>>& timestamp_scalar,
    arrow::TimeUnit::type time_unit) {
  ARROW_RETURN_NOT_OK(timestamp_scalar.status());
  return timestamp_scalar.ValueOrDie()->CastTo(arrow::timestamp(time_unit));
}

}  // namespace arrow_utils
}  // namespace stream_data_processor
