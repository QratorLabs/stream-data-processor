#include <spdlog/spdlog.h>

#include "last_aggregate_function.h"

#include "metadata/column_typing.h"
#include "utils/compute_utils.h"

arrow::Status LastAggregateFunction::aggregate(
    const std::shared_ptr<arrow::RecordBatch>& data,
    const std::string& column_name,
    std::shared_ptr<arrow::Scalar>* result) const {
  std::string time_column_name;
  ARROW_RETURN_NOT_OK(
      ColumnTyping::getTimeColumnNameMetadata(data, &time_column_name));

  auto ts_column = data->GetColumnByName(time_column_name);
  if (ts_column == nullptr) {
    return arrow::Status::Invalid(fmt::format(
        "RecordBatch has not time column with name {}", time_column_name));
  }

  std::pair<size_t, size_t> arg_min_max;
  ARROW_RETURN_NOT_OK(ComputeUtils::argMinMax(ts_column, &arg_min_max));

  auto get_scalar_result =
      data->GetColumnByName(column_name)->GetScalar(arg_min_max.second);
  if (!get_scalar_result.ok()) {
    return get_scalar_result.status();
  }

  *result = get_scalar_result.ValueOrDie();
  return arrow::Status::OK();
}
