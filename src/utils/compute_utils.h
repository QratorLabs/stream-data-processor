#pragma once

#include <deque>
#include <memory>
#include <utility>
#include <vector>

#include <arrow/api.h>

namespace stream_data_processor {
namespace compute_utils {

class ComputeException : public std::exception {
 public:
  explicit ComputeException(const std::string& message) : message_(message) {}

  [[nodiscard]] const char* what() const noexcept override {
    return message_.c_str();
  }

 private:
  std::string message_;
};

arrow::Result<arrow::RecordBatchVector> groupSortingByColumns(
    const std::vector<std::string>& column_names,
    const std::shared_ptr<arrow::RecordBatch>& record_batch);

arrow::Result<std::shared_ptr<arrow::RecordBatch>> sortByColumn(
    const std::string& column_name,
    const std::shared_ptr<arrow::RecordBatch>& source);

arrow::Result<std::pair<size_t, size_t>> argMinMax(
    std::shared_ptr<arrow::Array> array);

arrow::Result<size_t> tsLowerBound(const arrow::Array& sorted_ts_array,
                                   const std::function<bool(int64_t)>& pred,
                                   arrow::TimeUnit::type time_unit);

class DerivativeCalculator {
 public:
  virtual double calculateDerivative(const std::deque<double>& xs,
                                     const std::deque<double>& ys,
                                     double x_der, size_t order) const = 0;

  virtual ~DerivativeCalculator() = default;

 protected:
  DerivativeCalculator() = default;

  DerivativeCalculator(const DerivativeCalculator& /* non-used */) = default;
  DerivativeCalculator& operator=(
      const DerivativeCalculator& /* non-used */) = default;

  DerivativeCalculator(DerivativeCalculator&& /* non-used */) = default;
  DerivativeCalculator& operator=(DerivativeCalculator&& /* non-used */) =
      default;
};

class FDDerivativeCalculator : public DerivativeCalculator {
 public:
  enum LinearSolver {
    AUTO,
    PARTIAL_PIV_LU,
    HOUSEHOLDER_QR,
    COL_PIV_HOUSEHOLDER_QR
  };

 public:
  explicit FDDerivativeCalculator(LinearSolver linear_solver = AUTO)
      : linear_solver_(linear_solver) {}

  double calculateDerivative(const std::deque<double>& xs,
                             const std::deque<double>& ys, double x_der,
                             size_t order) const override;

 private:
  LinearSolver linear_solver_;
};

}  // namespace compute_utils
}  // namespace stream_data_processor
