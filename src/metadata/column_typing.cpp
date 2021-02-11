#include <spdlog/spdlog.h>

#include "column_typing.h"
#include "help.h"

namespace stream_data_processor {
namespace metadata {

namespace {

inline const std::string COLUMN_TYPE_METADATA_KEY{"column_type"};
inline const std::string TIME_COLUMN_NAME_METADATA_KEY{"time_column_name"};
inline const std::string MEASUREMENT_COLUMN_NAME_METADATA_KEY{
    "measurement_column_name"};

arrow::Status setColumnNameMetadata(
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

  ARROW_RETURN_NOT_OK(
      help::setSchemaMetadata(record_batch, metadata_key, column_name));

  return arrow::Status::OK();
}

}  // namespace

arrow::Status setColumnTypeMetadata(
    std::shared_ptr<arrow::Field>* column_field, ColumnType type) {
  ARROW_RETURN_NOT_OK(help::setFieldMetadata(
      column_field, COLUMN_TYPE_METADATA_KEY, ColumnType_Name(type)));

  return arrow::Status::OK();
}

arrow::Status setColumnTypeMetadata(
    std::shared_ptr<arrow::RecordBatch>* record_batch, int i,
    ColumnType type) {
  ARROW_RETURN_NOT_OK(help::setColumnMetadata(
      record_batch, i, COLUMN_TYPE_METADATA_KEY, ColumnType_Name(type)));

  return arrow::Status::OK();
}

arrow::Status setColumnTypeMetadata(
    std::shared_ptr<arrow::RecordBatch>* record_batch,
    const std::string& column_name, ColumnType type) {
  ARROW_RETURN_NOT_OK(help::setColumnMetadata(record_batch, column_name,
                                              COLUMN_TYPE_METADATA_KEY,
                                              ColumnType_Name(type)));

  return arrow::Status::OK();
}

ColumnType getColumnType(const arrow::Field& column_field) {
  ColumnType type = ColumnType::UNKNOWN;
  if (!column_field.HasMetadata()) {
    return type;
  }

  auto metadata = column_field.metadata();
  if (!metadata->Contains(COLUMN_TYPE_METADATA_KEY)) {
    return type;
  }

  ColumnType_Parse(metadata->Get(COLUMN_TYPE_METADATA_KEY).ValueOrDie(),
                   &type);

  return type;
}

arrow::Status setTimeColumnNameMetadata(
    std::shared_ptr<arrow::RecordBatch>* record_batch,
    const std::string& time_column_name) {
  ARROW_RETURN_NOT_OK(setColumnNameMetadata(record_batch, time_column_name,
                                            TIME_COLUMN_NAME_METADATA_KEY,
                                            arrow::Type::TIMESTAMP, TIME));

  return arrow::Status::OK();
}

arrow::Result<std::string> getTimeColumnNameMetadata(
    const arrow::RecordBatch& record_batch) {
  return help::getColumnNameMetadata(record_batch,
                                     TIME_COLUMN_NAME_METADATA_KEY);
}

arrow::Status setMeasurementColumnNameMetadata(
    std::shared_ptr<arrow::RecordBatch>* record_batch,
    const std::string& measurement_column_name) {
  ARROW_RETURN_NOT_OK(
      setColumnNameMetadata(record_batch, measurement_column_name,
                            MEASUREMENT_COLUMN_NAME_METADATA_KEY,
                            arrow::Type::STRING, MEASUREMENT));

  return arrow::Status::OK();
}

arrow::Result<std::string> getMeasurementColumnNameMetadata(
    const arrow::RecordBatch& record_batch) {
  return help::getColumnNameMetadata(record_batch,
                                     MEASUREMENT_COLUMN_NAME_METADATA_KEY);
}

arrow::Result<std::unordered_map<std::string, ColumnType>> getColumnTypes(
    const arrow::RecordBatch& record_batch) {
  std::unordered_map<std::string, ColumnType> column_types;
  for (auto& field : record_batch.schema()->fields()) {
    if (column_types.find(field->name()) != column_types.end()) {
      return arrow::Status::KeyError(
          fmt::format("Duplicate of column names: {}", field->name()));
    }

    column_types[field->name()] = getColumnType(*field);
  }

  return column_types;
}

arrow::Result<std::string> getMeasurement(
    const arrow::RecordBatch& record_batch) {
  ARROW_ASSIGN_OR_RAISE(auto measurement_column_name,
                        getMeasurementColumnNameMetadata(record_batch));

  auto measurement_column =
      record_batch.GetColumnByName(measurement_column_name);

  if (measurement_column == nullptr) {
    return arrow::Status::Invalid(
        fmt::format("Invalid measurement column name metadata: {}",
                    measurement_column_name));
  }

  if (record_batch.num_rows() == 0) {
    return arrow::Status::Invalid(
        "Can't extract measurement value from empty"
        "RecordBatch");
  }

  std::string measurement_value;
  ARROW_ASSIGN_OR_RAISE(auto measurement_scalar,
                        measurement_column->GetScalar(0));
  return measurement_scalar->ToString();
}

arrow::Result<std::string> getMeasurementAndValidate(
    const arrow::RecordBatch& record_batch) {
  ARROW_ASSIGN_OR_RAISE(auto measurement_value, getMeasurement(record_batch));

  ARROW_ASSIGN_OR_RAISE(auto measurement_column_name,
                        getMeasurementColumnNameMetadata(record_batch));

  auto measurement_column =
      record_batch.GetColumnByName(measurement_column_name);

  std::shared_ptr<arrow::Scalar> measurement_scalar;
  for (int i = 0; i < record_batch.num_rows(); ++i) {
    ARROW_ASSIGN_OR_RAISE(measurement_scalar,
                          measurement_column->GetScalar(i));
    if (measurement_value != measurement_scalar->ToString()) {
      return arrow::Status::Invalid(
          fmt::format("Found more than one unique measurement values: "
                      "{} and {}",
                      measurement_value, measurement_scalar->ToString()));
    }
  }

  return measurement_value;
}

}  // namespace metadata
}  // namespace stream_data_processor
