#pragma once

#include <iostream>

#include <arrow/api.h>
#include <catch2/catch.hpp>
#include <gmock/gmock.h>

#include "record_batch_handlers/record_batch_handler.h"

namespace sdp = stream_data_processor;

inline void arrowAssertNotOk(const arrow::Status& status) {
  INFO(status.message());
  REQUIRE( status.ok() );
}

template <typename ResultType>
inline void arrowAssignOrRaise(ResultType& lvalue, const arrow::Result<ResultType> result) {
  arrowAssertNotOk(result.status());
  lvalue = std::move(result.ValueOrDie());
}

inline void checkSize(const std::shared_ptr<arrow::RecordBatch>& record_batch, size_t num_rows, size_t num_columns) {
  REQUIRE( record_batch->num_rows() == num_rows );
  REQUIRE( record_batch->columns().size() == num_columns );
}

inline void checkColumnsArePresent(const std::shared_ptr<arrow::RecordBatch>& record_batch,
                            const std::vector<std::string>& column_names) {
  for (auto& column_name : column_names) {
    REQUIRE( record_batch->GetColumnByName(column_name) != nullptr );
  }
}

template<typename T, typename ArrowType>
inline void checkValue(const T& expected_value, const std::shared_ptr<arrow::RecordBatch>& record_batch,
                const std::string& column_name, size_t i) {
  std::shared_ptr<arrow::Scalar> field_scalar;
  arrowAssignOrRaise(field_scalar, record_batch->GetColumnByName(column_name)->GetScalar(i));
  REQUIRE( expected_value == std::static_pointer_cast<ArrowType>(field_scalar)->value );
}

template<>
inline void checkValue<std::string, arrow::StringScalar> (const std::string& expected_value,
                                                   const std::shared_ptr<arrow::RecordBatch>& record_batch,
                                                   const std::string& column_name, size_t i) {
  std::shared_ptr<arrow::Scalar> field_scalar;
  arrowAssignOrRaise(field_scalar, record_batch->GetColumnByName(column_name)->GetScalar(i));
  REQUIRE( std::static_pointer_cast<arrow::StringScalar>(field_scalar)->value->ToString() == expected_value );
}


template<typename T, typename ArrowType>
[[ nodiscard ]] inline bool equals(const T& expected_value, const std::shared_ptr<arrow::RecordBatch>& record_batch,
            const std::string& column_name, size_t i) {
  auto field_result = record_batch->GetColumnByName(column_name)->GetScalar(i);
  if (!field_result.ok()) {
    std::cerr << field_result.status() << std::endl;
    return false;
  }

  return expected_value == std::static_pointer_cast<ArrowType>(field_result.ValueOrDie())->value;
}

template<>
[[ nodiscard ]] inline bool equals<std::string, arrow::StringScalar> (const std::string& expected_value,
                                               const std::shared_ptr<arrow::RecordBatch>& record_batch,
                                               const std::string& column_name, size_t i) {
  auto field_result = record_batch->GetColumnByName(column_name)->GetScalar(i);
  if (!field_result.ok()) {
    std::cerr << field_result.status() << std::endl;
    return false;
  }

  return expected_value == std::static_pointer_cast<arrow::StringScalar>(field_result.ValueOrDie())->value->ToString();
}

inline void checkIsNull(const std::shared_ptr<arrow::RecordBatch>& record_batch,
                 const std::string& column_name, size_t i) {
  std::shared_ptr<arrow::Scalar> field_scalar;
  arrowAssignOrRaise(field_scalar, record_batch->GetColumnByName(column_name)->GetScalar(i));
  REQUIRE( !field_scalar->is_valid );
}

inline void checkIsValid(const std::shared_ptr<arrow::RecordBatch>& record_batch,
                  const std::string& column_name, size_t i) {
  std::shared_ptr<arrow::Scalar> field_scalar;
  arrowAssignOrRaise(field_scalar, record_batch->GetColumnByName(column_name)->GetScalar(i));
  REQUIRE( field_scalar->is_valid );
}

template <class ExpectedType, class ExactType>
[[ nodiscard ]] inline bool instanceOf(ExactType* object) {
  return dynamic_cast<const ExpectedType*>(object) != nullptr;
}

inline void assertMetadataIsEqual(
    arrow::RecordBatch& expected,
    arrow::RecordBatch& actual) {
  REQUIRE( expected.schema()->Equals(actual.schema(), true) );
  for (auto& expected_field : expected.schema()->fields()) {
    auto actual_field = actual.schema()->GetFieldByName(expected_field->name());
    REQUIRE( actual_field != nullptr );
    REQUIRE( expected_field->Equals(actual_field, true) );
  }
}

class MockRecordBatchHandler : public sdp::RecordBatchHandler {
 public:
  MOCK_METHOD(arrow::Result<arrow::RecordBatchVector>,
              handle,
              (const std::shared_ptr<arrow::RecordBatch>& record_batch),
              (override));

  MOCK_METHOD(arrow::Result<arrow::RecordBatchVector>,
              handle,
              (const arrow::RecordBatchVector& record_batches),
              (override));
};
