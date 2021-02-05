#include <arrow/compute/api.h>

#include "compute_utils.h"

namespace stream_data_processor {
namespace compute_utils {

namespace {

arrow::Status sort(
    const std::vector<std::string>& column_names, size_t i,
    const std::shared_ptr<arrow::RecordBatch>& source,
    std::vector<std::shared_ptr<arrow::RecordBatch>>* targets) {
  if (i == column_names.size()) {
    targets->push_back(source);
    return arrow::Status::OK();
  }

  if (source->GetColumnByName(column_names[i]) == nullptr) {
    ARROW_RETURN_NOT_OK(sort(column_names, i + 1, source, targets));
    return arrow::Status::OK();
  }

  std::shared_ptr<arrow::RecordBatch> sorted_batch;
  ARROW_ASSIGN_OR_RAISE(sorted_batch, sortByColumn(column_names[i], source));
  while (true) {
    auto sorted_keys = sorted_batch->GetColumnByName(column_names[i]);

    std::shared_ptr<arrow::Scalar> min_val;
    ARROW_ASSIGN_OR_RAISE(min_val, sorted_keys->GetScalar(0));

    std::shared_ptr<arrow::Scalar> max_val;
    ARROW_ASSIGN_OR_RAISE(max_val,
                          sorted_keys->GetScalar(sorted_keys->length() - 1));

    arrow::Datum equals_datum;
    ARROW_ASSIGN_OR_RAISE(
        equals_datum,
        arrow::compute::Compare(sorted_keys, min_val,
                                arrow::compute::CompareOptions(
                                    arrow::compute::CompareOperator::EQUAL)));

    arrow::Datum filter_datum;
    ARROW_ASSIGN_OR_RAISE(filter_datum,
                          arrow::compute::Filter(sorted_batch, equals_datum));

    ARROW_RETURN_NOT_OK(
        sort(column_names, i + 1, filter_datum.record_batch(), targets));

    if (min_val->Equals(max_val)) {
      break;
    }

    arrow::Datum not_equals_datum;
    ARROW_ASSIGN_OR_RAISE(
        not_equals_datum,
        arrow::compute::Compare(
            sorted_keys, min_val,
            arrow::compute::CompareOptions(
                arrow::compute::CompareOperator::NOT_EQUAL)));

    arrow::Datum filter_not_equals_datum;
    ARROW_ASSIGN_OR_RAISE(
        filter_not_equals_datum,
        arrow::compute::Filter(sorted_batch, not_equals_datum));

    sorted_batch = filter_not_equals_datum.record_batch();
  }

  return arrow::Status::OK();
}

}  // namespace

arrow::Result<arrow::RecordBatchVector> groupSortingByColumns(
    const std::vector<std::string>& column_names,
    const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  arrow::RecordBatchVector grouped;
  ARROW_RETURN_NOT_OK(sort(column_names, 0, record_batch, &grouped));
  return grouped;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> sortByColumn(
    const std::string& column_name,
    const std::shared_ptr<arrow::RecordBatch>& source) {
  auto sorting_column = source->GetColumnByName(column_name);
  if (sorting_column->type_id() == arrow::Type::TIMESTAMP) {
    ARROW_ASSIGN_OR_RAISE(sorting_column,
                          sorting_column->View(arrow::int64()));
  }

  std::shared_ptr<arrow::Array> sorted_idx;
  ARROW_ASSIGN_OR_RAISE(sorted_idx,
                        arrow::compute::SortIndices(*sorting_column));

  arrow::Datum sorted_datum;
  ARROW_ASSIGN_OR_RAISE(sorted_datum,
                        arrow::compute::Take(source, sorted_idx));

  return sorted_datum.record_batch();
}

arrow::Result<std::pair<size_t, size_t>> argMinMax(
    std::shared_ptr<arrow::Array> array) {
  if (array->type_id() == arrow::Type::TIMESTAMP) {
    ARROW_ASSIGN_OR_RAISE(array, array->View(arrow::int64()));
  }

  arrow::Datum min_max_ts;
  ARROW_ASSIGN_OR_RAISE(min_max_ts, arrow::compute::MinMax(array));

  int64_t arg_min = -1;
  int64_t arg_max = -1;
  size_t i = 0;
  while (i < array->length() && (arg_min == -1 || arg_max == -1)) {
    std::shared_ptr<arrow::Scalar> value_scalar;
    ARROW_ASSIGN_OR_RAISE(value_scalar, array->GetScalar(i));
    if (value_scalar->Equals(
            min_max_ts.scalar_as<arrow::StructScalar>().value[0])) {
      arg_min = i;
    }

    if (value_scalar->Equals(
            min_max_ts.scalar_as<arrow::StructScalar>().value[1])) {
      arg_max = i;
    }

    ++i;
  }

  return std::pair{arg_min, arg_max};
}

}  // namespace compute_utils
}  // namespace stream_data_processor
