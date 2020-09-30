#include <arrow/compute/api.h>

#include "compute_utils.h"

arrow::Status ComputeUtils::groupSortingByColumns(const std::vector<std::string>& column_names,
                                                  const std::shared_ptr<arrow::RecordBatch>& record_batch,
                                                  std::vector<std::shared_ptr<arrow::RecordBatch>>* grouped) {
  return sort(column_names, 0, record_batch, grouped);
}

arrow::Status ComputeUtils::sortByColumn(const std::string& column_name,
                                         const std::shared_ptr<arrow::RecordBatch>& source,
                                         std::shared_ptr<arrow::RecordBatch>* target) {
  auto sorting_column = source->GetColumnByName(column_name);
  if (sorting_column->type_id() == arrow::Type::TIMESTAMP) {
    auto converted_sorting_column_result = sorting_column->View(arrow::int64());
    if (!converted_sorting_column_result.ok()) {
      return converted_sorting_column_result.status();
    }

    sorting_column = converted_sorting_column_result.ValueOrDie();
  }

  auto sorted_idx_result = arrow::compute::SortToIndices(*sorting_column);
  if (!sorted_idx_result.ok()) {
    return sorted_idx_result.status();
  }

  auto sorted_record_batch_result = arrow::compute::Take(source, sorted_idx_result.ValueOrDie());
  if (!sorted_record_batch_result.ok()) {
    return sorted_record_batch_result.status();
  }

  *target = sorted_record_batch_result.ValueOrDie().record_batch();
  return arrow::Status::OK();
}

arrow::Status ComputeUtils::argMinMax(std::shared_ptr<arrow::Array> array, std::pair<size_t, size_t>* arg_min_max) {
  if (array->type_id() == arrow::Type::TIMESTAMP) {
    auto converted_sorting_column_result = array->View(arrow::int64());
    if (!converted_sorting_column_result.ok()) {
      return converted_sorting_column_result.status();
    }

    array = converted_sorting_column_result.ValueOrDie();
  }

  auto min_max_ts_result = arrow::compute::MinMax(array);
  if (!min_max_ts_result.ok()) {
    return min_max_ts_result.status();
  }

  int64_t arg_min = -1;
  int64_t arg_max = -1;
  size_t i = 0;
  while (i < array->length() && (arg_min == -1 || arg_max == -1)) {
    auto get_scalar_result = array->GetScalar(i);
    if (!get_scalar_result.ok()) {
      return get_scalar_result.status();
    }

    if (get_scalar_result.ValueOrDie()->Equals(min_max_ts_result.ValueOrDie().scalar_as<arrow::StructScalar>().value[0])) {
      arg_min = i;
    }

    if (get_scalar_result.ValueOrDie()->Equals(min_max_ts_result.ValueOrDie().scalar_as<arrow::StructScalar>().value[1])) {
      arg_max = i;
    }

    ++i;
  }

  *arg_min_max = {arg_min, arg_max};
  return arrow::Status::OK();
}

arrow::Status ComputeUtils::sort(const std::vector<std::string>& column_names,
                                 size_t i,
                                 const std::shared_ptr<arrow::RecordBatch>& source,
                                 std::vector<std::shared_ptr<arrow::RecordBatch>>* targets) {
  if (i == column_names.size()) {
    targets->push_back(source);
    return arrow::Status::OK();
  }

  std::shared_ptr<arrow::RecordBatch> sorted_batch;
  ARROW_RETURN_NOT_OK(sortByColumn(column_names[i], source, &sorted_batch));
  while (true) {
    auto sorted_keys = sorted_batch->GetColumnByName(column_names[i]);

    auto min_val = sorted_keys->GetScalar(0);
    if (!min_val.ok()) {
      return min_val.status();
    }

    auto max_val = sorted_keys->GetScalar(sorted_keys->length() - 1);
    if (!max_val.ok()) {
      return max_val.status();
    }

    auto equals_result = arrow::compute::Compare(sorted_keys, min_val.ValueOrDie(),
                                                 arrow::compute::CompareOptions(arrow::compute::CompareOperator::EQUAL));
    if (!equals_result.ok()) {
      return equals_result.status();
    }

    auto filter_equals_result = arrow::compute::Filter(sorted_batch, equals_result.ValueOrDie());
    if (!filter_equals_result.ok()) {
      return filter_equals_result.status();
    }

    ARROW_RETURN_NOT_OK(sort(column_names, i + 1, filter_equals_result.ValueOrDie().record_batch(), targets));
    if (min_val.ValueOrDie()->Equals(max_val.ValueOrDie())) {
      break;
    }

    auto not_equals_result = arrow::compute::Compare(sorted_keys, min_val.ValueOrDie(),
                                                     arrow::compute::CompareOptions(arrow::compute::CompareOperator::NOT_EQUAL));
    if (!not_equals_result.ok()) {
      return equals_result.status();
    }

    auto filter_not_equals_result = arrow::compute::Filter(sorted_batch, not_equals_result.ValueOrDie());
    if (!filter_not_equals_result.ok()) {
      return filter_not_equals_result.status();
    }

    sorted_batch = filter_not_equals_result.ValueOrDie().record_batch();
  }

  return arrow::Status::OK();
}
