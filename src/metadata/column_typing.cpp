#include <spdlog/spdlog.h>

#include "column_typing.h"

const std::string ColumnTyping::COLUMN_TYPE_METADATA_KEY{"column_type"};
const std::string ColumnTyping::TIME_COLUMN_NAME_METADATA_KEY{
    "time_column_name"};
const std::string ColumnTyping::MEASUREMENT_COLUMN_NAME_METADATA_KEY{
    "measurement_column_name"};

arrow::Status ColumnTyping::setColumnTypeMetadata(
    std::shared_ptr<arrow::Field>* column_field, ColumnType type) {
  std::shared_ptr<arrow::KeyValueMetadata> arrow_metadata = nullptr;
  if (column_field->get()->HasMetadata()) {
    arrow_metadata = column_field->get()->metadata()->Copy();
  } else {
    arrow_metadata = std::make_shared<arrow::KeyValueMetadata>();
  }

  ARROW_RETURN_NOT_OK(
      arrow_metadata->Set(COLUMN_TYPE_METADATA_KEY, ColumnType_Name(type)));

  *column_field = column_field->get()->WithMetadata(arrow_metadata);
  return arrow::Status::OK();
}

arrow::Status ColumnTyping::setColumnTypeMetadata(
    std::shared_ptr<arrow::RecordBatch>* record_batch, int i,
    ColumnType type) {
  if (i < 0 || i >= record_batch->get()->num_columns()) {
    return arrow::Status::IndexError(
        fmt::format("Column index {} is out of bounds", i));
  }

  auto field = record_batch->get()->schema()->field(i);
  ARROW_RETURN_NOT_OK(setColumnTypeMetadata(&field, type));
  ARROW_RETURN_NOT_OK(record_batch->get()->schema()->SetField(i, field));

  return arrow::Status::OK();
}

arrow::Status ColumnTyping::setColumnTypeMetadata(
    std::shared_ptr<arrow::RecordBatch>* record_batch,
    std::string column_name, ColumnType type) {
  auto i = record_batch->get()->schema()->GetFieldIndex(column_name);
  if (i == -1) {
    return arrow::Status::KeyError(
        fmt::format("No such column: {}", column_name));
  }

  ARROW_RETURN_NOT_OK(setColumnTypeMetadata(record_batch, i, type));
  return arrow::Status::OK();
}

ColumnType ColumnTyping::getColumnType(
    const std::shared_ptr<arrow::Field>& column_field) {
  ColumnType type = ColumnType::UNKNOWN;
  if (!column_field->HasMetadata()) {
    return type;
  }

  auto metadata = column_field->metadata();
  if (!metadata->Contains(COLUMN_TYPE_METADATA_KEY)) {
    return type;
  }

  ColumnType_Parse(metadata->Get(COLUMN_TYPE_METADATA_KEY).ValueOrDie(),
                   &type);

  return type;
}

arrow::Status ColumnTyping::setTimeColumnNameMetadata(
    std::shared_ptr<arrow::RecordBatch>* record_batch,
    const std::string& time_column_name) {
  ARROW_RETURN_NOT_OK(setColumnNameMetadata(record_batch, time_column_name,
                                            TIME_COLUMN_NAME_METADATA_KEY,
                                            arrow::Type::TIMESTAMP, TIME));

  return arrow::Status::OK();
}

arrow::Status ColumnTyping::getTimeColumnNameMetadata(
    const std::shared_ptr<arrow::RecordBatch>& record_batch,
    std::string* time_column_name) {
  ARROW_RETURN_NOT_OK(getColumnNameMetadata(
      record_batch, TIME_COLUMN_NAME_METADATA_KEY, time_column_name));

  return arrow::Status::OK();
}

arrow::Status ColumnTyping::setMeasurementColumnNameMetadata(
    std::shared_ptr<arrow::RecordBatch>* record_batch,
    const std::string& measurement_column_name) {
  ARROW_RETURN_NOT_OK(
      setColumnNameMetadata(record_batch, measurement_column_name,
                            MEASUREMENT_COLUMN_NAME_METADATA_KEY,
                            arrow::Type::STRING, MEASUREMENT));

  return arrow::Status::OK();
}

arrow::Status ColumnTyping::getMeasurementColumnNameMetadata(
    const std::shared_ptr<arrow::RecordBatch>& record_batch,
    std::string* measurement_column_name) {
  ARROW_RETURN_NOT_OK(getColumnNameMetadata(
      record_batch, MEASUREMENT_COLUMN_NAME_METADATA_KEY,
      measurement_column_name));

  return arrow::Status::OK();
}

arrow::Status ColumnTyping::getColumnNameMetadata(
    const std::shared_ptr<arrow::RecordBatch>& record_batch,
    const std::string& metadata_key, std::string* column_name) {
  auto metadata = record_batch->schema()->metadata();
  if (metadata == nullptr) {
    return arrow::Status::Invalid(
        "RecordBatch has no metadata to extract corresponded column name");
  }

  if (!metadata->Contains(metadata_key)) {
    return arrow::Status::KeyError(
        fmt::format("RecordBatch's metadata has no key {} to extract "
                    "corresponded column name",
                    metadata_key));
  }

  *column_name = metadata->Get(metadata_key).ValueOrDie();
  return arrow::Status::OK();
}
arrow::Status ColumnTyping::setColumnNameMetadata(
    std::shared_ptr<arrow::RecordBatch>* record_batch,
    const std::string& column_name, const std::string& metadata_key,
    arrow::Type::type arrow_column_type, ColumnType column_type) {
  auto column = record_batch->get()->GetColumnByName(column_name);
  if (column == nullptr) {
    return arrow::Status::KeyError(fmt::format(
        "No such column to set {} metadata: {}", metadata_key, column_name));
  }

  if (column->type_id() != arrow_column_type) {
    return arrow::Status::Invalid(fmt::format(
        "Column {} must have {} arrow type", column_name, arrow_column_type));
  }

  ARROW_RETURN_NOT_OK(
      setColumnTypeMetadata(record_batch, column_name, column_type));

  std::shared_ptr<arrow::KeyValueMetadata> arrow_metadata = nullptr;
  if (record_batch->get()->schema()->HasMetadata()) {
    arrow_metadata = record_batch->get()->schema()->metadata()->Copy();
  } else {
    arrow_metadata = std::make_shared<arrow::KeyValueMetadata>();
  }

  ARROW_RETURN_NOT_OK(arrow_metadata->Set(metadata_key, column_name));
  *record_batch = record_batch->get()->ReplaceSchemaMetadata(arrow_metadata);

  return arrow::Status::OK();
}
