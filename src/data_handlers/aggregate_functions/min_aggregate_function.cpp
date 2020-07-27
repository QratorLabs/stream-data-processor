#include <arrow/compute/api.h>

#include "min_aggregate_function.h"

arrow::Status MinAggregateFunction::aggregate(const std::shared_ptr<arrow::RecordBatch> &data,
                                              const std::string &column_name,
                                              std::shared_ptr<arrow::Scalar> *result,
                                              const std::string &ts_column_name) const {
  auto min_max_result = arrow::compute::MinMax(data->GetColumnByName(column_name));
  if (!min_max_result.ok()) {
    return min_max_result.status();
  }

  *result = min_max_result.ValueOrDie().scalar_as<arrow::StructScalar>().value[0];
  return arrow::Status::OK();
}
