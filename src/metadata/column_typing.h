#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include <arrow/api.h>

#include "metadata.pb.h"

namespace stream_data_processor {
namespace metadata {

arrow::Status setColumnTypeMetadata(
    std::shared_ptr<arrow::Field>* column_field, ColumnType type);

arrow::Status setColumnTypeMetadata(
    std::shared_ptr<arrow::RecordBatch>* record_batch, int i,
    ColumnType type);

arrow::Status setColumnTypeMetadata(
    std::shared_ptr<arrow::RecordBatch>* record_batch,
    const std::string& column_name, ColumnType type);

ColumnType getColumnType(const arrow::Field& column_field);

arrow::Status setTimeColumnNameMetadata(
    std::shared_ptr<arrow::RecordBatch>* record_batch,
    const std::string& time_column_name);

arrow::Result<std::string> getTimeColumnNameMetadata(
    const arrow::RecordBatch& record_batch);

arrow::Status setMeasurementColumnNameMetadata(
    std::shared_ptr<arrow::RecordBatch>* record_batch,
    const std::string& measurement_column_name);

arrow::Result<std::string> getMeasurementColumnNameMetadata(
    const arrow::RecordBatch& record_batch);

arrow::Result<std::unordered_map<std::string, ColumnType>> getColumnTypes(
    const arrow::RecordBatch& record_batch);

arrow::Result<std::string> getMeasurement(
    const arrow::RecordBatch& record_batch);

arrow::Result<std::string> getMeasurementAndValidate(
    const arrow::RecordBatch& record_batch);

}  // namespace metadata
}  // namespace stream_data_processor
