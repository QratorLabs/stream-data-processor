#pragma once

#include <chrono>
#include <deque>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <arrow/api.h>

#include "handler_factory.h"
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

  struct DerivativeOptions {
    std::chrono::nanoseconds unit_time_segment;
    std::chrono::nanoseconds derivative_neighbourhood;
    std::unordered_map<std::string, DerivativeCase> derivative_cases;
    bool no_wait_future{false};
  };

 public:
  template <typename DerivativeCalculatorType, typename OptionsType>
  DerivativeHandler(DerivativeCalculatorType&& derivative_calculator,
                    OptionsType&& options)
      : derivative_calculator_(
            std::forward<DerivativeCalculatorType>(derivative_calculator)),
        options_(std::forward<OptionsType>(options)) {}

  arrow::Result<arrow::RecordBatchVector> handle(
      const std::shared_ptr<arrow::RecordBatch>& record_batch) override;

 private:
  struct BufferedValues {
    std::deque<double> times;
    std::deque<double> values;
  };

 private:
  arrow::Result<double> getScaledPositionTime(
      int64_t row_id, const arrow::Array& time_column) const;

 private:
  std::shared_ptr<DerivativeCalculator> derivative_calculator_;
  DerivativeOptions options_;
  std::deque<double> all_buffered_times_;
  std::unordered_map<std::string, BufferedValues> buffered_values_;
  std::shared_ptr<arrow::RecordBatch> buffered_batch_{nullptr};
};

class DerivativeHandlerFactory : public HandlerFactory {
 public:
  template <typename DerivativeCalculatorType, typename OptionsType>
  DerivativeHandlerFactory(DerivativeCalculatorType&& derivative_calculator,
                           OptionsType&& options)
      : derivative_calculator_(
            std::forward<DerivativeCalculatorType>(derivative_calculator)),
        options_(std::forward<OptionsType>(options)) {}

  std::shared_ptr<RecordBatchHandler> createHandler() const override;

 private:
  std::shared_ptr<DerivativeCalculator> derivative_calculator_;
  DerivativeHandler::DerivativeOptions options_;
};

}  // namespace stream_data_processor
