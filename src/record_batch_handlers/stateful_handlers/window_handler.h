#pragma once

#include <chrono>
#include <deque>
#include <optional>
#include <string>
#include <utility>

#include "handler_factory.h"
#include "record_batch_handlers/record_batch_handler.h"
#include "utils/utils.h"

namespace stream_data_processor {

class IWindowHandler : public RecordBatchHandler {
 public:
  virtual std::chrono::seconds getPeriodOption() const = 0;
  virtual std::chrono::seconds getEveryOption() const = 0;

  virtual void setPeriodOption(std::chrono::seconds new_period_option,
                               std::time_t change_ts) = 0;
  virtual void setEveryOption(std::chrono::seconds new_every_option,
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

  std::chrono::seconds getPeriodOption() const override {
    return options_.period;
  }

  std::chrono::seconds getEveryOption() const override {
    return options_.every;
  }

  void setPeriodOption(std::chrono::seconds new_period_option,
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

  void setEveryOption(std::chrono::seconds new_every_option,
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

 private:
  arrow::Result<arrow::RecordBatchVector> emitWindow();
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
    std::optional<std::string> period_column_name;
    std::optional<std::string> every_column_name;
  };

  template <typename OptionsType>
  DynamicWindowHandler(std::shared_ptr<IWindowHandler> window_handler,
                       OptionsType&& options)
      : window_handler_(std::move(window_handler)),
        options_(std::forward<OptionsType>(options)) {}

  arrow::Result<arrow::RecordBatchVector> handle(
      const std::shared_ptr<arrow::RecordBatch>& record_batch) override;

 private:
  arrow::Result<int64_t> findNewWindowOptionIndex(
      const arrow::RecordBatch& record_batch,
      std::chrono::seconds current_option_value,
      const std::string& option_column_name,
      time_utils::TimeUnit option_time_unit) const;

  static arrow::Result<std::chrono::seconds> getDurationOption(
      const arrow::RecordBatch& record_batch, int64_t row,
      const std::string& column_name, time_utils::TimeUnit time_unit);

  static arrow::Result<time_utils::TimeUnit> getColumnTimeUnit(
      const arrow::RecordBatch& record_batch, const std::string& column_name);

 private:
  std::shared_ptr<IWindowHandler> window_handler_;
  DynamicWindowOptions options_;
};

class DynamicWindowHandlerFactory : public HandlerFactory {
 public:
  template <class WindowOptionsType, class DynamicWindowOptionsType>
  explicit DynamicWindowHandlerFactory(
      WindowOptionsType&& window_options,
      DynamicWindowOptionsType&& dynamic_window_options)
      : window_options_(std::forward<WindowOptionsType>(window_options)),
        dynamic_window_options_(
            std::forward<DynamicWindowOptionsType>(dynamic_window_options)) {}

  std::shared_ptr<RecordBatchHandler> createHandler() const override;

 private:
  WindowHandler::WindowOptions window_options_;
  DynamicWindowHandler::DynamicWindowOptions dynamic_window_options_;
};

}  // namespace stream_data_processor
