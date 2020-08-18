#pragma once

#include <iostream>

#include <arrow/api.h>

#include <catch2/catch.hpp>

inline void arrowAssertNotOk(const arrow::Status& status) {
  if (!status.ok()) {
    INFO(status.ToString());
    FAIL();
  }

  SUCCEED();
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
  auto field_result = record_batch->GetColumnByName(column_name)->GetScalar(i);
  arrowAssertNotOk(field_result.status());
  REQUIRE( std::static_pointer_cast<ArrowType>(field_result.ValueOrDie())->value == expected_value );
}

template<>
inline void checkValue<std::string, arrow::StringScalar> (const std::string& expected_value,
                                                   const std::shared_ptr<arrow::RecordBatch>& record_batch,
                                                   const std::string& column_name, size_t i) {
  auto field_result = record_batch->GetColumnByName(column_name)->GetScalar(i);
  arrowAssertNotOk(field_result.status());
  REQUIRE( std::static_pointer_cast<arrow::StringScalar>(field_result.ValueOrDie())->value->ToString() == expected_value );
}


template<typename T, typename ArrowType>
inline bool equals(const T& expected_value, const std::shared_ptr<arrow::RecordBatch>& record_batch,
            const std::string& column_name, size_t i) {
  auto field_result = record_batch->GetColumnByName(column_name)->GetScalar(i);
  if (!field_result.ok()) {
    std::cerr << field_result.status() << std::endl;
    return false;
  }

  return expected_value == std::static_pointer_cast<ArrowType>(field_result.ValueOrDie())->value;
}

template<>
inline bool equals<std::string, arrow::StringScalar> (const std::string& expected_value,
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
  auto field_result = record_batch->GetColumnByName(column_name)->GetScalar(i);
  arrowAssertNotOk(field_result.status());
  REQUIRE( !field_result.ValueOrDie()->is_valid );
}

inline void checkIsValid(const std::shared_ptr<arrow::RecordBatch>& record_batch,
                  const std::string& column_name, size_t i) {
  auto field_result = record_batch->GetColumnByName(column_name)->GetScalar(i);
  arrowAssertNotOk(field_result.status());
  REQUIRE( field_result.ValueOrDie()->is_valid );
}
