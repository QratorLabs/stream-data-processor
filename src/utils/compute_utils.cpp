#include <unordered_set>

#include <arrow/compute/api.h>
#include <spdlog/spdlog.h>
#include <Eigen/Dense>

#include "arrow_utils.h"
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

  ARROW_ASSIGN_OR_RAISE(auto sorted_batch,
                        sortByColumn(column_names[i], source));
  while (true) {
    auto sorted_keys = sorted_batch->GetColumnByName(column_names[i]);

    ARROW_ASSIGN_OR_RAISE(auto min_val, sorted_keys->GetScalar(0));

    ARROW_ASSIGN_OR_RAISE(auto max_val,
                          sorted_keys->GetScalar(sorted_keys->length() - 1));

    arrow::Datum equals_datum;
    ARROW_ASSIGN_OR_RAISE(
        equals_datum,
        arrow::compute::Compare(sorted_keys, min_val,
                                arrow::compute::CompareOptions(
                                    arrow::compute::CompareOperator::EQUAL)));

    ARROW_ASSIGN_OR_RAISE(auto filter_datum,
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
  if (sorting_column == nullptr) {
    return arrow::Status::KeyError(
        fmt::format("No such column with name {}", column_name));
  }

  if (sorting_column->type_id() == arrow::Type::TIMESTAMP) {
    ARROW_ASSIGN_OR_RAISE(sorting_column,
                          sorting_column->View(arrow::int64()));
  }

  ARROW_ASSIGN_OR_RAISE(auto sorted_idx,
                        arrow::compute::SortIndices(*sorting_column));

  ARROW_ASSIGN_OR_RAISE(auto sorted_datum,
                        arrow::compute::Take(source, sorted_idx));

  return sorted_datum.record_batch();
}

arrow::Result<std::pair<size_t, size_t>> argMinMax(
    std::shared_ptr<arrow::Array> array) {
  if (array->type_id() == arrow::Type::TIMESTAMP) {
    ARROW_ASSIGN_OR_RAISE(array, array->View(arrow::int64()));
  }

  ARROW_ASSIGN_OR_RAISE(auto min_max_ts, arrow::compute::MinMax(array));

  int64_t arg_min = -1;
  int64_t arg_max = -1;
  size_t i = 0;
  while (i < array->length() && (arg_min == -1 || arg_max == -1)) {
    ARROW_ASSIGN_OR_RAISE(auto value_scalar, array->GetScalar(i));
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

arrow::Result<size_t> tsLowerBound(const arrow::Array& sorted_ts_array,
                                   const std::function<bool(int64_t)>& pred,
                                   arrow::TimeUnit::type time_unit) {
  size_t left_bound = 0;
  size_t right_bound = sorted_ts_array.length();
  while (left_bound != right_bound - 1) {
    auto middle = (left_bound + right_bound) / 2;

    ARROW_ASSIGN_OR_RAISE(auto ts_scalar,
                          arrow_utils::castTimestampScalar(
                              sorted_ts_array.GetScalar(middle), time_unit));

    int64_t ts =
        std::static_pointer_cast<arrow::TimestampScalar>(ts_scalar)->value;

    if (pred(ts)) {
      right_bound = middle;
    } else {
      left_bound = middle;
    }
  }

  ARROW_ASSIGN_OR_RAISE(
      auto ts_scalar, arrow_utils::castTimestampScalar(
                          sorted_ts_array.GetScalar(left_bound), time_unit));

  int64_t ts =
      std::static_pointer_cast<arrow::TimestampScalar>(ts_scalar)->value;

  if (pred(ts)) {
    return left_bound;
  } else {
    return right_bound;
  }
}

double FDDerivativeCalculator::calculateDerivative(
    const std::deque<double>& xs, const std::deque<double>& ys, double x_der,
    size_t order) const {
  if (xs.size() != ys.size()) {
    throw ComputeException(
        fmt::format("Argument and value arrays have different sizes: {} and "
                    "{}",
                    xs.size(), ys.size()));
  }

  if (xs.size() < order + 1) {
    throw ComputeException(
        fmt::format("For calculating {}-order derivative at least {} values "
                    "are needed",
                    order, order + 1));
  }

  std::unordered_set<double> xs_set;
  Eigen::MatrixXd taylor_coeffs_matrix(xs.size(), xs.size());
  for (size_t k = 0; k < xs.size(); ++k) {
    if (xs_set.find(xs[k]) != xs_set.end()) {
      throw ComputeException(fmt::format(
          "Found repeating argument {} which is not allowed", xs[k]));
    } else {
      xs_set.insert(xs[k]);
    }

    double delta = xs[k] - x_der;
    double coeff = 1;
    taylor_coeffs_matrix(0, k) = coeff;
    for (int64_t n = 1; n < xs.size(); ++n) {
      coeff *= delta / n;
      taylor_coeffs_matrix(n, k) = coeff;
    }
  }

  Eigen::VectorXd result_vector = Eigen::VectorXd::Zero(xs.size());
  result_vector(order) = 1;

  Eigen::VectorXd der_coeffs;

  switch (linear_solver_) {
    case AUTO:
    case PARTIAL_PIV_LU:
      der_coeffs = taylor_coeffs_matrix.partialPivLu().solve(result_vector);
      break;
    case HOUSEHOLDER_QR:
      der_coeffs = taylor_coeffs_matrix.householderQr().solve(result_vector);
      break;
    case COL_PIV_HOUSEHOLDER_QR:
      der_coeffs =
          taylor_coeffs_matrix.colPivHouseholderQr().solve(result_vector);
      break;
    default:
      throw ComputeException(
          fmt::format("Unexpected linear solver type: {}", linear_solver_));
  }

  double der_result = 0;
  for (size_t i = 0; i < ys.size(); ++i) {
    der_result += ys[i] * der_coeffs(i);
  }

  return der_result;
}

}  // namespace compute_utils
}  // namespace stream_data_processor
