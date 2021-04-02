#pragma once

#include <chrono>
#include <deque>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <arrow/api.h>

#include "record_batch_handlers/record_batch_handler.h"
#include "utils/compute_utils.h"

namespace stream_data_processor {

using compute_utils::DerivativeCalculator;

class DerivativeHandler : public RecordBatchHandler {
 public:
  struct DerivativeCase {
    std::string values_column_name;
    size_t order;
  };

 public:
  template <typename CasesMapType>
  DerivativeHandler(
      std::unique_ptr<DerivativeCalculator>&& derivative_calculator,
      std::chrono::nanoseconds unit_time_segment,
      std::chrono::nanoseconds derivative_neighbourhood, CasesMapType&& cases)
      : derivative_calculator_(std::move(derivative_calculator)),
        unit_time_segment_(unit_time_segment),
        derivative_neighbourhood_(derivative_neighbourhood),
        derivative_cases_(std::forward<CasesMapType>(cases)) {}

  arrow::Result<arrow::RecordBatchVector> handle(
      const std::shared_ptr<arrow::RecordBatch>& record_batch) override;

 private:
  arrow::Result<double> getScaledPositionTime(
      int64_t row_id, const arrow::Array& time_column) const;

  arrow::Result<double> getPositionValue(
      int64_t row_id, const arrow::Array& value_column) const;

 private:
  std::unique_ptr<DerivativeCalculator> derivative_calculator_;
  std::chrono::nanoseconds unit_time_segment_;
  std::chrono::nanoseconds derivative_neighbourhood_;
  std::unordered_map<std::string, DerivativeCase> derivative_cases_;
  std::deque<double> buffered_times_;
  std::unordered_map<std::string, std::deque<double>> buffered_values_;
  std::shared_ptr<arrow::RecordBatch> buffered_batch_{nullptr};
};

}  // namespace stream_data_processor
