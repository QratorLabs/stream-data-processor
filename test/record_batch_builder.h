#pragma once

#include <string>
#include <vector>
#include <type_traits>

#include <arrow/api.h>
#include <spdlog/spdlog.h>

#include "metadata/column_typing.h"

namespace sdp = stream_data_processor;

class RecordBatchBuilder {
 public:
  void reset();

  arrow::Status setRowNumber(int row_number);

  template <typename ValueType>
  arrow::Status buildTimeColumn(
      const std::string& time_column_name,
      const std::vector<ValueType>& values,
      arrow::TimeUnit::type time_unit) {
    static_assert(std::is_integral_v<ValueType>,
                  "Time values must have integral type at least");

    ARROW_RETURN_NOT_OK(checkValueArraySize(values));
    if (specified_metadata_columns_.find(SPECIFIED_TIME_KEY) !=
        specified_metadata_columns_.end()) {
      return arrow::Status::ExecutionError("Time column has already built");
    }

    specified_metadata_columns_[SPECIFIED_TIME_KEY] = time_column_name;

    fields_.push_back(arrow::field(
        time_column_name, arrow::timestamp(time_unit)));

    ARROW_RETURN_NOT_OK(sdp::metadata::setColumnTypeMetadata(
        &fields_.back(), sdp::metadata::TIME));

    arrow::TimestampBuilder column_builder(
        arrow::timestamp(time_unit), arrow::default_memory_pool());

    for (auto& time_value : values) {
      ARROW_RETURN_NOT_OK(column_builder.Append(time_value));
    }

    column_arrays_.emplace_back();
    ARROW_RETURN_NOT_OK(column_builder.Finish(&column_arrays_.back()));

    return arrow::Status::OK();
  }

  arrow::Status buildMeasurementColumn(
      const std::string& measurement_column_name,
      const std::vector<std::string>& values);

  template <typename ValueType>
  arrow::Status buildColumn(
      const std::string& column_name,
      const std::vector<ValueType>& values,
      sdp::metadata::ColumnType column_type = sdp::metadata::UNKNOWN,
      const std::vector<bool>& is_valid = {});

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> getResult() const;

 private:
  template <typename ValueType>
  arrow::Status checkValueArraySize(std::vector<ValueType> values) {
    if (row_number_ == -1) {
      return arrow::Status::Invalid("Row number didn't set yet");
    }

    if (row_number_ != values.size()) {
      return arrow::Status::Invalid(fmt::format(
          "Row number set to value: {}, values provided: {}",
          row_number_, values.size()));
    }

    return arrow::Status::OK();
  }

 private:
  const static std::string SPECIFIED_MEASUREMENT_KEY;
  const static std::string SPECIFIED_TIME_KEY;

  arrow::FieldVector fields_;
  arrow::ArrayVector column_arrays_;
  std::unordered_map<std::string, std::string> specified_metadata_columns_;
  int row_number_{-1};
};


