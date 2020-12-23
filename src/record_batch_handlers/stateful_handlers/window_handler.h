#pragma once

#include <chrono>
#include <deque>
#include <string>
#include <utility>

#include "record_batch_handlers/record_batch_handler.h"
#include "utils/utils.h"

namespace stream_data_processor {

class IWindowHandler : public RecordBatchHandler {
 public:
  virtual std::chrono::seconds getEveryOption() const = 0;
  virtual std::chrono::seconds getPeriodOption() const = 0;

  virtual void setEveryOption(const std::chrono::seconds& new_every_option,
                              std::time_t change_ts) = 0;
  virtual void setPeriodOption(const std::chrono::seconds& new_period_option,
                               std::time_t change_ts) = 0;
};

class WindowHandler : public IWindowHandler {
 public:
  struct WindowOptions {
    std::chrono::seconds period;
    std::chrono::seconds every;
    bool fill_period{false};
  };

  template <typename OptionsType>
  explicit WindowHandler(OptionsType&& options)
      : options_(std::forward<OptionsType>(options)) {}

  arrow::Result<arrow::RecordBatchVector> handle(
      const std::shared_ptr<arrow::RecordBatch>& record_batch) override;

  std::chrono::seconds getEveryOption() const override {
    return options_.every;
  }

  std::chrono::seconds getPeriodOption() const override {
    return options_.period;
  }

  void setEveryOption(const std::chrono::seconds& new_every_option,
                      std::time_t change_ts) override {
    if (next_emit_ != 0 && (emitted_first_ || !options_.fill_period) &&
        next_emit_ + new_every_option.count() >= options_.every.count() &&
        next_emit_ + new_every_option.count() - options_.every.count() >=
            change_ts) {
      next_emit_ =
          next_emit_ + new_every_option.count() - options_.every.count();
    }

    options_.every = new_every_option;
  }

  void setPeriodOption(const std::chrono::seconds& new_period_option,
                       std::time_t change_ts) override {
    if (next_emit_ != 0 && !emitted_first_ && options_.fill_period &&
        next_emit_ + new_period_option.count() >= options_.period.count() &&
        next_emit_ + new_period_option.count() - options_.period.count() >=
            change_ts) {
      next_emit_ =
          next_emit_ + new_period_option.count() - options_.period.count();
    }

    options_.period = new_period_option;
  }

 private:
  arrow::Result<size_t> tsLowerBound(
      const arrow::RecordBatch& record_batch,
      const std::function<bool(std::time_t)>& pred,
      const std::string& time_column_name);

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> emitWindow();
  arrow::Status removeOldRecords();

 private:
  WindowOptions options_;
  std::deque<std::shared_ptr<arrow::RecordBatch>> buffered_record_batches_;
  std::time_t next_emit_{0};
  bool emitted_first_{false};
};

class DynamicWindowHandler : public RecordBatchHandler {
 public:
  struct DynamicWindowOptions {
    std::string period_column_name;
    std::string every_column_name;
  };

  template <typename OptionsType>
  DynamicWindowHandler(OptionsType&& options,
                       std::shared_ptr<IWindowHandler> window_handler)
      : options_(std::forward<OptionsType>(options)),
        window_handler_(std::move(window_handler)) {}

  arrow::Result<arrow::RecordBatchVector> handle(
      const std::shared_ptr<arrow::RecordBatch>& record_batch) override;

 private:
  arrow::Result<int64_t> findNewWindowOptionsIndex(
      const arrow::RecordBatch& record_batch,
      time_utils::TimeUnit every_time_unit,
      time_utils::TimeUnit period_time_unit) const;

  static arrow::Result<std::chrono::seconds> getDurationOption(
      const arrow::RecordBatch& record_batch, int64_t row,
      const std::string& column_name, time_utils::TimeUnit time_unit);

  static arrow::Result<time_utils::TimeUnit> getColumnTimeUnit(
      const arrow::RecordBatch& record_batch, const std::string& column_name);

 private:
  DynamicWindowOptions options_;
  std::shared_ptr<IWindowHandler> window_handler_;
};

}  // namespace stream_data_processor
