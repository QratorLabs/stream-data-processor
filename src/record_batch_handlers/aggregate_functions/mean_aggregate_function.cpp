#include <arrow/compute/api.h>

#include "mean_aggregate_function.h"

arrow::Status MeanAggregateFunction::aggregate(
    const std::shared_ptr<arrow::RecordBatch>& data,
    const std::string& column_name, std::shared_ptr<arrow::Scalar>* result,
    const std::string& ts_column_name) const {
  auto mean_result = arrow::compute::Mean(data->GetColumnByName(column_name));
  if (!mean_result.ok()) {
    return mean_result.status();
  }

  *result = mean_result.ValueOrDie().scalar();
  return arrow::Status::OK();
}
