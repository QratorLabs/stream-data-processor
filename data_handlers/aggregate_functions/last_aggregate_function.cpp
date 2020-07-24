#include "last_aggregate_function.h"

#include "utils/compute_utils.h"

arrow::Status LastAggregateFunction::aggregate(const std::shared_ptr<arrow::RecordBatch> &data,
                                               const std::string &column_name,
                                               std::shared_ptr<arrow::Scalar> *result,
                                               const std::string &ts_column_name) const {
  if (ts_column_name.empty()) {
    return arrow::Status::Invalid("There should be a valid timestamp field for \"last\" aggregate function");
  }

  auto ts_column = data->GetColumnByName(ts_column_name);
  std::pair<size_t, size_t> arg_min_max;
  ARROW_RETURN_NOT_OK(ComputeUtils::argMinMax(ts_column, arg_min_max));

  auto get_scalar_result = data->GetColumnByName(column_name)->GetScalar(arg_min_max.second);
  if (!get_scalar_result.ok()) {
    return get_scalar_result.status();
  }

  *result = get_scalar_result.ValueOrDie();
  return arrow::Status::OK();
}
