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

 private:
  static const std::string COLUMN_TYPE_METADATA_KEY;
  static const std::string TIME_COLUMN_NAME_METADATA_KEY;
};
