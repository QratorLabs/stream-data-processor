#pragma once

#include <chrono>
#include <ctime>
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
      const std::shared_ptr<arrow::RecordBatch>& record_batch, int row_id,
      arrow::DoubleBuilder* threshold_column_builder) = 0;

 protected:
  arrow::Status getColumnValueAtRow(
      const std::shared_ptr<arrow::RecordBatch>& record_batch,
      const std::string& column_name, int row_id, double* value);

  arrow::Status getTimeAtRow(
      const std::shared_ptr<arrow::RecordBatch>& record_batch, int row_id,
      std::time_t* time);
};

class StateOK : public ThresholdState {
 public:
  StateOK(const std::shared_ptr<ThresholdStateMachine>& state_machine,
          double current_threshold);

  StateOK(const std::weak_ptr<ThresholdStateMachine>& state_machine,
          double current_threshold);

  arrow::Status addThresholdForRow(
      const std::shared_ptr<arrow::RecordBatch>& record_batch, int row_id,
      arrow::DoubleBuilder* threshold_column_builder) override;

 private:
  std::weak_ptr<ThresholdStateMachine> state_machine_;
  double current_threshold_;
};

class StateAlert : public ThresholdState {
 public:
  StateAlert(const std::shared_ptr<ThresholdStateMachine>& state_machine,
             double current_threshold, std::time_t alert_start);

  StateAlert(const std::weak_ptr<ThresholdStateMachine>& state_machine,
             double current_threshold, std::time_t alert_start);

  arrow::Status addThresholdForRow(
      const std::shared_ptr<arrow::RecordBatch>& record_batch, int row_id,
      arrow::DoubleBuilder* threshold_column_builder) override;

 private:
  std::weak_ptr<ThresholdStateMachine> state_machine_;
  double current_threshold_;
  std::time_t alert_start_;
};

class StateRelax : public ThresholdState {
 public:
  StateRelax(const std::shared_ptr<ThresholdStateMachine>& state_machine,
             double current_threshold, std::time_t relax_start);

  StateRelax(const std::weak_ptr<ThresholdStateMachine>& state_machine,
             double current_threshold, std::time_t relax_start);

  arrow::Status addThresholdForRow(
      const std::shared_ptr<arrow::RecordBatch>& record_batch, int row_id,
      arrow::DoubleBuilder* threshold_column_builder) override;

 private:
  std::weak_ptr<ThresholdStateMachine> state_machine_;
  double current_threshold_;
  std::time_t relax_start_;
};

class StateDecrease : public ThresholdState {
 public:
  StateDecrease(const std::shared_ptr<ThresholdStateMachine>& state_machine,
                double current_threshold, std::time_t decrease_start);

  StateDecrease(const std::weak_ptr<ThresholdStateMachine>& state_machine,
                double current_threshold, std::time_t decrease_start);

  arrow::Status addThresholdForRow(
      const std::shared_ptr<arrow::RecordBatch>& record_batch, int row_id,
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
    std::chrono::seconds alert_duration;

    std::chrono::seconds relax_duration;

    double decrease_trigger_factor{0};
    double decrease_scale_factor{0};
    std::chrono::seconds decrease_duration{0};

    metadata::ColumnType threshold_column_type{metadata::FIELD};
  };

  template <typename OptionsType>
  explicit ThresholdStateMachine(OptionsType&& options)
      : options_(std::forward<OptionsType>(options)) {}

  arrow::Status handle(
      const std::shared_ptr<arrow::RecordBatch>& record_batch,
      arrow::RecordBatchVector* result) override;

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
