#include <arrow/compute/api.h>
#include <spdlog/spdlog.h>

#include "aggregate_functions.h"
#include "metadata/column_typing.h"
#include "utils/compute_utils.h"

namespace stream_data_processor {

arrow::Result<std::shared_ptr<arrow::Scalar>>
FirstAggregateFunction::aggregate(const arrow::RecordBatch& data,
                                  const std::string& column_name) const {
  ARROW_ASSIGN_OR_RAISE(auto time_column_name,
                        metadata::getTimeColumnNameMetadata(data));

  auto ts_column = data.GetColumnByName(time_column_name);
  if (ts_column == nullptr) {
    return arrow::Status::Invalid(fmt::format(
        "RecordBatch has not time column with name {}", time_column_name));
  }

  ARROW_ASSIGN_OR_RAISE(auto arg_min_max,
                        compute_utils::argMinMax(ts_column));

  return data.GetColumnByName(column_name)->GetScalar(arg_min_max.first);
}

arrow::Result<std::shared_ptr<arrow::Scalar>>
LastAggregateFunction::aggregate(const arrow::RecordBatch& data,
                                 const std::string& column_name) const {
  ARROW_ASSIGN_OR_RAISE(auto time_column_name,
                        metadata::getTimeColumnNameMetadata(data));

  auto ts_column = data.GetColumnByName(time_column_name);
  if (ts_column == nullptr) {
    return arrow::Status::Invalid(fmt::format(
        "RecordBatch has not time column with name {}", time_column_name));
  }

  ARROW_ASSIGN_OR_RAISE(auto arg_min_max,
                        compute_utils::argMinMax(ts_column));

  return data.GetColumnByName(column_name)->GetScalar(arg_min_max.second);
}

arrow::Result<std::shared_ptr<arrow::Scalar>> MaxAggregateFunction::aggregate(
    const arrow::RecordBatch& data, const std::string& column_name) const {
  arrow::Datum min_max;
  ARROW_ASSIGN_OR_RAISE(
      min_max, arrow::compute::MinMax(data.GetColumnByName(column_name)));

  return min_max.scalar_as<arrow::StructScalar>().value[1];
}

arrow::Result<std::shared_ptr<arrow::Scalar>>
MeanAggregateFunction::aggregate(const arrow::RecordBatch& data,
                                 const std::string& column_name) const {
  arrow::Datum mean;
  ARROW_ASSIGN_OR_RAISE(
      mean, arrow::compute::Mean(data.GetColumnByName(column_name)));

  return mean.scalar();
}

arrow::Result<std::shared_ptr<arrow::Scalar>> MinAggregateFunction::aggregate(
    const arrow::RecordBatch& data, const std::string& column_name) const {
  arrow::Datum min_max;
  ARROW_ASSIGN_OR_RAISE(
      min_max, arrow::compute::MinMax(data.GetColumnByName(column_name)));

  return min_max.scalar_as<arrow::StructScalar>().value[0];
}

}  // namespace stream_data_processor
