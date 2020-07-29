#pragma once

#include <iostream>

#include <arrow/api.h>

#include <gtest/gtest.h>

::testing::AssertionResult arrowAssertNotOk(const arrow::Status& status) {
  if (!status.ok()) {
    return ::testing::AssertionFailure() << status.ToString();
  }

  return ::testing::AssertionSuccess();
}

void checkSize(const std::shared_ptr<arrow::RecordBatch>& record_batch, size_t num_rows, size_t num_columns) {
  ASSERT_EQ(num_columns, record_batch->columns().size());
  ASSERT_EQ(num_rows, record_batch->num_rows());
}

void checkColumnsArePresent(const std::shared_ptr<arrow::RecordBatch>& record_batch,
                            const std::vector<std::string>& column_names) {
  for (auto& column_name : column_names) {
    ASSERT_NE(nullptr, record_batch->GetColumnByName(column_name));
  }
}

template<typename T, typename ArrowType>
void checkValue(const T& expected_value, const std::shared_ptr<arrow::RecordBatch>& record_batch,
                const std::string& column_name, size_t i) {
  auto field_result = record_batch->GetColumnByName(column_name)->GetScalar(i);
  ASSERT_TRUE(arrowAssertNotOk(field_result.status()));
  ASSERT_EQ(expected_value, std::static_pointer_cast<ArrowType>(field_result.ValueOrDie())->value);
}

template<>
void checkValue<std::string, arrow::StringScalar> (const std::string& expected_value,
                                                   const std::shared_ptr<arrow::RecordBatch>& record_batch,
                                                   const std::string& column_name, size_t i) {
  auto field_result = record_batch->GetColumnByName(column_name)->GetScalar(i);
  ASSERT_TRUE(arrowAssertNotOk(field_result.status()));
  ASSERT_EQ(expected_value, std::static_pointer_cast<arrow::StringScalar>(field_result.ValueOrDie())->value->ToString());
}


template<typename T, typename ArrowType>
bool equals(const T& expected_value, const std::shared_ptr<arrow::RecordBatch>& record_batch,
            const std::string& column_name, size_t i) {
  auto field_result = record_batch->GetColumnByName(column_name)->GetScalar(i);
  if (!field_result.ok()) {
    std::cerr << field_result.status() << std::endl;
    return false;
  }

  return expected_value == std::static_pointer_cast<ArrowType>(field_result.ValueOrDie())->value;
}

template<>
bool equals<std::string, arrow::StringScalar> (const std::string& expected_value,
                                               const std::shared_ptr<arrow::RecordBatch>& record_batch,
                                               const std::string& column_name, size_t i) {
  auto field_result = record_batch->GetColumnByName(column_name)->GetScalar(i);
  if (!field_result.ok()) {
    std::cerr << field_result.status() << std::endl;
    return false;
  }

  return expected_value == std::static_pointer_cast<arrow::StringScalar>(field_result.ValueOrDie())->value->ToString();
}

void checkIsInvalid(const std::shared_ptr<arrow::RecordBatch>& record_batch,
                    const std::string& column_name, size_t i) {
  auto field_result = record_batch->GetColumnByName(column_name)->GetScalar(i);
  ASSERT_TRUE(arrowAssertNotOk(field_result.status()));
  ASSERT_TRUE(!field_result.ValueOrDie()->is_valid);
}

void checkIsValid(const std::shared_ptr<arrow::RecordBatch>& record_batch,
                  const std::string& column_name, size_t i) {
  auto field_result = record_batch->GetColumnByName(column_name)->GetScalar(i);
  ASSERT_TRUE(arrowAssertNotOk(field_result.status()));
  ASSERT_TRUE(field_result.ValueOrDie()->is_valid);
}
