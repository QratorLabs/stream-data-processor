#pragma once

#include <chrono>
#include <ctime>
#include <limits>
#include <memory>
#include <string>

#include <arrow/api.h>

#include "handler_factory.h"
#include "metadata/column_typing.h"
#include "record_batch_handlers/record_batch_handler.h"

namespace stream_data_processor {

class ThresholdStateMachine;

namespace internal {

class ThresholdState {
 public:
  virtual arrow::Status addThresholdForRow(
      const arrow::RecordBatch& record_batch, int row_id,
      arrow::DoubleBuilder* threshold_column_builder) = 0;

  virtual ~ThresholdState() = 0;

 protected:
  arrow::Result<double> getColumnValueAtRow(
      const arrow::RecordBatch& record_batch, const std::string& column_name,
      int row_id);

  arrow::Result<std::time_t> getTimeAtRow(
      const arrow::RecordBatch& record_batch, int row_id);

  ThresholdState() = default;

  ThresholdState(const ThresholdState& /* non-used */) = default;
  ThresholdState& operator=(const ThresholdState& /* non-used */) = default;

  ThresholdState(ThresholdState&& /* non-used */) = default;
  ThresholdState& operator=(ThresholdState&& /* non-used */) = default;
};

class StateOK : public ThresholdState {
 public:
  StateOK(const std::shared_ptr<ThresholdStateMachine>& state_machine,
          double current_threshold);

  StateOK(std::weak_ptr<ThresholdStateMachine> state_machine,
          double current_threshold);

  arrow::Status addThresholdForRow(
      const arrow::RecordBatch& record_batch, int row_id,
      arrow::DoubleBuilder* threshold_column_builder) override;

 private:
  std::weak_ptr<ThresholdStateMachine> state_machine_;
  double current_threshold_;
};

class StateIncrease : public ThresholdState {
 public:
  StateIncrease(const std::shared_ptr<ThresholdStateMachine>& state_machine,
                double current_threshold, std::time_t alert_start);

  StateIncrease(std::weak_ptr<ThresholdStateMachine> state_machine,
                double current_threshold, std::time_t alert_start);

  arrow::Status addThresholdForRow(
      const arrow::RecordBatch& record_batch, int row_id,
      arrow::DoubleBuilder* threshold_column_builder) override;

 private:
  std::weak_ptr<ThresholdStateMachine> state_machine_;
  double current_threshold_;
  std::time_t alert_start_;
};

class StateDecrease : public ThresholdState {
 public:
  StateDecrease(const std::shared_ptr<ThresholdStateMachine>& state_machine,
                double current_threshold, std::time_t decrease_start);

  StateDecrease(std::weak_ptr<ThresholdStateMachine> state_machine,
                double current_threshold, std::time_t decrease_start);

  arrow::Status addThresholdForRow(
      const arrow::RecordBatch& record_batch, int row_id,
      arrow::DoubleBuilder* threshold_column_builder) override;

 private:
  std::weak_ptr<ThresholdStateMachine> state_machine_;
  double current_threshold_;
  std::time_t decrease_start_;
};

}  // namespace internal

class ThresholdStateMachine : public RecordBatchHandler {
 public:
  struct Options {
    std::string watch_column_name;
    std::string threshold_column_name;

    double default_threshold;

    double increase_scale_factor;
    std::chrono::seconds increase_after;

    double decrease_trigger_factor{0};
    double decrease_scale_factor{0};
    std::chrono::seconds decrease_after{0};

    double min_threshold{0};
    double max_threshold{std::numeric_limits<double>::max()};

    metadata::ColumnType threshold_column_type{metadata::FIELD};
  };

  template <typename OptionsType>
  explicit ThresholdStateMachine(OptionsType&& options)
      : options_(std::forward<OptionsType>(options)) {}

  arrow::Result<arrow::RecordBatchVector> handle(
      const std::shared_ptr<arrow::RecordBatch>& record_batch) override;

  void changeState(
      std::shared_ptr<internal::ThresholdState> new_threshold_state);

  const Options& getOptions() const;
  std::shared_ptr<const internal::ThresholdState> getState() const;

 private:
  Options options_;
  std::shared_ptr<internal::ThresholdState> state_{nullptr};
};

class ThresholdStateMachineFactory : public HandlerFactory {
 public:
  template <typename OptionsType>
  explicit ThresholdStateMachineFactory(OptionsType&& options)
      : options_(std::forward<OptionsType>(options)) {}

  std::shared_ptr<RecordBatchHandler> createHandler() const override;

 private:
  ThresholdStateMachine::Options options_;
};

}  // namespace stream_data_processor
