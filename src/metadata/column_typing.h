#pragma once

#include <memory>
#include <string>

#include <arrow/api.h>

#include "metadata.pb.h"

class ColumnTyping {
 public:
  static arrow::Status setColumnTypeMetadata(
      std::shared_ptr<arrow::Field>* column_field, ColumnType type);

  static arrow::Status setColumnTypeMetadata(
      std::shared_ptr<arrow::RecordBatch>* record_batch, int i,
      ColumnType type);

  static arrow::Status setColumnTypeMetadata(
      std::shared_ptr<arrow::RecordBatch>* record_batch,
      std::string column_name, ColumnType type);

  static ColumnType getColumnType(
      const std::shared_ptr<arrow::Field>& column_field);

  static arrow::Status setTimeColumnNameMetadata(
      std::shared_ptr<arrow::RecordBatch>* record_batch,
      const std::string& time_column_name);

  static arrow::Status getTimeColumnNameMetadata(
      const std::shared_ptr<arrow::RecordBatch>& record_batch,
      std::string* time_column_name);

  static arrow::Status setMeasurementColumnNameMetadata(
      std::shared_ptr<arrow::RecordBatch>* record_batch,
      const std::string& measurement_column_name);

  static arrow::Status getMeasurementColumnNameMetadata(
      const std::shared_ptr<arrow::RecordBatch>& record_batch,
      std::string* measurement_column_name);

 private:
  static arrow::Status setColumnNameMetadata(
      std::shared_ptr<arrow::RecordBatch>* record_batch,
      const std::string& column_name, const std::string& metadata_key,
      arrow::Type::type arrow_column_type, ColumnType column_type);

  static arrow::Status getColumnNameMetadata(
      const std::shared_ptr<arrow::RecordBatch>& record_batch,
      const std::string& metadata_key, std::string* column_name);

 private:
  static const std::string COLUMN_TYPE_METADATA_KEY;
  static const std::string TIME_COLUMN_NAME_METADATA_KEY;
  static const std::string MEASUREMENT_COLUMN_NAME_METADATA_KEY;
};
