#include "arrow_utils.h"

namespace stream_data_processor {
namespace arrow_utils {

arrow::Status makeArrayBuilder(arrow::Type::type type,
                               std::shared_ptr<arrow::ArrayBuilder>* builder,
                               arrow::MemoryPool* pool) {
  switch (type) {
    case arrow::Type::INT64:
      *builder = std::make_shared<arrow::Int64Builder>(pool);
      return arrow::Status::OK();
    case arrow::Type::DOUBLE:
      *builder = std::make_shared<arrow::DoubleBuilder>(pool);
      return arrow::Status::OK();
    case arrow::Type::STRING:
      *builder = std::make_shared<arrow::StringBuilder>(pool);
      return arrow::Status::OK();
    case arrow::Type::BOOL:
      *builder = std::make_shared<arrow::BooleanBuilder>(pool);
      return arrow::Status::OK();
    case arrow::Type::TIMESTAMP:
      *builder = std::make_shared<arrow::TimestampBuilder>(
          arrow::timestamp(arrow::TimeUnit::SECOND), pool);
      return arrow::Status::OK();
    default:
      return arrow::Status::NotImplemented(
          "Step-by-step array building currently supports one of "
          "{arrow::int64, arrow::float64, arrow::utf8, arrow::boolean, "
          "arrow::Type::TIMESTAMP} types fields "
          "only");  // TODO: support any type
  }
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
    case arrow::Type::TIMESTAMP:
      ARROW_RETURN_NOT_OK(
          std::static_pointer_cast<arrow::TimestampBuilder>(*builder)->Append(
              std::static_pointer_cast<arrow::TimestampScalar>(value)
                  ->value));
      return arrow::Status::OK();
    default:
      return arrow::Status::NotImplemented(
          "Expected one of {arrow::int64, arrow::float64, arrow::utf8, "
          "arrow::boolean, "
          "arrow::Type::TIMESTAMP} "
          "types");  // TODO: support any type
  }
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

}  // namespace arrow_utils
}  // namespace stream_data_processor
