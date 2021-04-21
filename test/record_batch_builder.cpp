#include <ctime>
#include <numeric>

#include "record_batch_builder.h"

#include "metadata/column_typing.h"

namespace sdp = stream_data_processor;

const std::string RecordBatchBuilder::SPECIFIED_TIME_KEY{"time"};
const std::string RecordBatchBuilder::SPECIFIED_MEASUREMENT_KEY{"measurement"};

void RecordBatchBuilder::reset() {
  row_number_ = -1;
  fields_.clear();
  column_arrays_.clear();
  specified_metadata_columns_.clear();
}

arrow::Status RecordBatchBuilder::setRowNumber(int row_number) {
  if (row_number_ != -1) {
    return arrow::Status::ExecutionError(
        "Row number is not reset, can't set new value");
  }

  row_number_ = row_number;
  return arrow::Status::OK();
}

template<>
arrow::Status RecordBatchBuilder::buildColumn<int64_t>(
    const std::string& column_name,
    const std::vector<int64_t>& values,
    sdp::metadata::ColumnType column_type,
    const std::vector<bool>& is_valid) {
  ARROW_RETURN_NOT_OK(checkValueArraySize(values));
  fields_.push_back(arrow::field(column_name, arrow::int64()));

  ARROW_RETURN_NOT_OK(sdp::metadata::setColumnTypeMetadata(
      &fields_.back(), column_type));

  arrow::Int64Builder column_builder;
  if (!is_valid.empty()) {
    ARROW_RETURN_NOT_OK(column_builder.AppendValues(values, is_valid));
  } else {
    ARROW_RETURN_NOT_OK(column_builder.AppendValues(values));
  }

  column_arrays_.emplace_back();
  ARROW_RETURN_NOT_OK(column_builder.Finish(&column_arrays_.back()));

  return arrow::Status::OK();
}

template<>
arrow::Status RecordBatchBuilder::buildColumn<std::string>(
    const std::string& column_name,
    const std::vector<std::string>& values,
    sdp::metadata::ColumnType column_type,
    const std::vector<bool>& is_valid) {
  ARROW_RETURN_NOT_OK(checkValueArraySize(values));
  fields_.push_back(arrow::field(column_name, arrow::utf8()));

  ARROW_RETURN_NOT_OK(sdp::metadata::setColumnTypeMetadata(
      &fields_.back(), column_type));

  arrow::StringBuilder column_builder;

  if (!is_valid.empty()) {
    uint8_t* valid_bytes = new uint8_t[values.size()];
    for (size_t i = 0; i < is_valid.size(); ++i) {
      if (is_valid[i]) {
        valid_bytes[i] = 1;
      } else {
        valid_bytes[i] = 0;
      }
    }

    ARROW_RETURN_NOT_OK(column_builder.AppendValues(values, valid_bytes));
    delete [] valid_bytes;
  } else {
    ARROW_RETURN_NOT_OK(column_builder.AppendValues(values));
  }

  column_arrays_.emplace_back();
  ARROW_RETURN_NOT_OK(column_builder.Finish(&column_arrays_.back()));

  return arrow::Status::OK();
}

template<>
arrow::Status RecordBatchBuilder::buildColumn<double>(
    const std::string& column_name,
    const std::vector<double>& values,
    sdp::metadata::ColumnType column_type,
    const std::vector<bool>& is_valid) {
  ARROW_RETURN_NOT_OK(checkValueArraySize(values));
  fields_.push_back(arrow::field(column_name, arrow::float64()));

  ARROW_RETURN_NOT_OK(sdp::metadata::setColumnTypeMetadata(
      &fields_.back(), column_type));

  arrow::DoubleBuilder column_builder;
  if (!is_valid.empty()) {
    ARROW_RETURN_NOT_OK(column_builder.AppendValues(values, is_valid));
  } else {
    ARROW_RETURN_NOT_OK(column_builder.AppendValues(values));
  }

  column_arrays_.emplace_back();
  ARROW_RETURN_NOT_OK(column_builder.Finish(&column_arrays_.back()));

  return arrow::Status::OK();
}

template<>
arrow::Status RecordBatchBuilder::buildColumn<bool>(
    const std::string& column_name,
    const std::vector<bool>& values,
    sdp::metadata::ColumnType column_type,
    const std::vector<bool>& is_valid) {
  ARROW_RETURN_NOT_OK(checkValueArraySize(values));
  fields_.push_back(arrow::field(column_name, arrow::boolean()));

  ARROW_RETURN_NOT_OK(sdp::metadata::setColumnTypeMetadata(
      &fields_.back(), column_type));

  arrow::BooleanBuilder column_builder;
  if (!is_valid.empty()) {
    ARROW_RETURN_NOT_OK(column_builder.AppendValues(values, is_valid));
  } else {
    ARROW_RETURN_NOT_OK(column_builder.AppendValues(values));
  }

  column_arrays_.emplace_back();
  ARROW_RETURN_NOT_OK(column_builder.Finish(&column_arrays_.back()));

  return arrow::Status::OK();
}

arrow::Status RecordBatchBuilder::buildMeasurementColumn(
    const std::string& measurement_column_name,
    const std::vector<std::string>& values) {
  if (specified_metadata_columns_.find(SPECIFIED_MEASUREMENT_KEY) !=
      specified_metadata_columns_.end()) {
    return arrow::Status::ExecutionError("Measurement column has already built");
  }

  specified_metadata_columns_[SPECIFIED_MEASUREMENT_KEY] = measurement_column_name;

  ARROW_RETURN_NOT_OK(buildColumn(
      measurement_column_name, values, sdp::metadata::MEASUREMENT));

  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> RecordBatchBuilder::getResult() const {
  auto record_batch = arrow::RecordBatch::Make(
      arrow::schema(fields_), row_number_, column_arrays_);

  if (specified_metadata_columns_.find(SPECIFIED_TIME_KEY) !=
        specified_metadata_columns_.end()) {
    ARROW_RETURN_NOT_OK(sdp::metadata::setTimeColumnNameMetadata(
        &record_batch, specified_metadata_columns_.at(SPECIFIED_TIME_KEY)));
  }

  if (specified_metadata_columns_.find(SPECIFIED_MEASUREMENT_KEY) !=
        specified_metadata_columns_.end()) {
    ARROW_RETURN_NOT_OK(sdp::metadata::setMeasurementColumnNameMetadata(
        &record_batch, specified_metadata_columns_.at(SPECIFIED_MEASUREMENT_KEY)));
  }

  return record_batch;
}
