#include <arrow/compute/api.h>
#include <spdlog/spdlog.h>

#include "aggregate_functions.h"
#include "metadata/column_typing.h"
#include "utils/compute_utils.h"

namespace stream_data_processor {

arrow::Status FirstAggregateFunction::aggregate(
    const arrow::RecordBatch& data, const std::string& column_name,
    std::shared_ptr<arrow::Scalar>* result) const {
  std::string time_column_name;
  ARROW_RETURN_NOT_OK(
      metadata::getTimeColumnNameMetadata(data, &time_column_name));

  auto ts_column = data.GetColumnByName(time_column_name);
  if (ts_column == nullptr) {
    return arrow::Status::Invalid(fmt::format(
        "RecordBatch has not time column with name {}", time_column_name));
  }

  std::pair<size_t, size_t> arg_min_max;
  ARROW_RETURN_NOT_OK(compute_utils::argMinMax(ts_column, &arg_min_max));

  auto get_scalar_result =
      data.GetColumnByName(column_name)->GetScalar(arg_min_max.first);
  if (!get_scalar_result.ok()) {
    return get_scalar_result.status();
  }

  *result = get_scalar_result.ValueOrDie();
  return arrow::Status::OK();
}

arrow::Status LastAggregateFunction::aggregate(
    const arrow::RecordBatch& data, const std::string& column_name,
    std::shared_ptr<arrow::Scalar>* result) const {
  std::string time_column_name;
  ARROW_RETURN_NOT_OK(
      metadata::getTimeColumnNameMetadata(data, &time_column_name));

  auto ts_column = data.GetColumnByName(time_column_name);
  if (ts_column == nullptr) {
    return arrow::Status::Invalid(fmt::format(
        "RecordBatch has not time column with name {}", time_column_name));
  }

  std::pair<size_t, size_t> arg_min_max;
  ARROW_RETURN_NOT_OK(compute_utils::argMinMax(ts_column, &arg_min_max));

  auto get_scalar_result =
      data.GetColumnByName(column_name)->GetScalar(arg_min_max.second);
  if (!get_scalar_result.ok()) {
    return get_scalar_result.status();
  }

  *result = get_scalar_result.ValueOrDie();
  return arrow::Status::OK();
}

arrow::Status MaxAggregateFunction::aggregate(
    const arrow::RecordBatch& data, const std::string& column_name,
    std::shared_ptr<arrow::Scalar>* result) const {
  auto min_max_result =
      arrow::compute::MinMax(data.GetColumnByName(column_name));
  if (!min_max_result.ok()) {
    return min_max_result.status();
  }

  *result =
      min_max_result.ValueOrDie().scalar_as<arrow::StructScalar>().value[1];
  return arrow::Status::OK();
}

arrow::Status MeanAggregateFunction::aggregate(
    const arrow::RecordBatch& data, const std::string& column_name,
    std::shared_ptr<arrow::Scalar>* result) const {
  auto mean_result = arrow::compute::Mean(data.GetColumnByName(column_name));
  if (!mean_result.ok()) {
    return mean_result.status();
  }

  *result = mean_result.ValueOrDie().scalar();
  return arrow::Status::OK();
}

arrow::Status MinAggregateFunction::aggregate(
    const arrow::RecordBatch& data, const std::string& column_name,
    std::shared_ptr<arrow::Scalar>* result) const {
  auto min_max_result =
      arrow::compute::MinMax(data.GetColumnByName(column_name));
  if (!min_max_result.ok()) {
    return min_max_result.status();
  }

  *result =
      min_max_result.ValueOrDie().scalar_as<arrow::StructScalar>().value[0];
  return arrow::Status::OK();
}

}  // namespace stream_data_processor
