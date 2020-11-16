#include <algorithm>

#include <spdlog/spdlog.h>

#include "metadata/column_typing.h"
#include "threshold_state_machine.h"
#include "utils/arrow_utils.h"

namespace stream_data_processor {

namespace internal {

arrow::Status ThresholdState::getColumnValueAtRow(
    const std::shared_ptr<arrow::RecordBatch>& record_batch,
    const std::string& column_name, int row_id, double* value) {
  auto column = record_batch->GetColumnByName(column_name);
  if (column == nullptr) {
    return arrow::Status::KeyError(
        fmt::format("Can't get value from column {}: no such column exists",
                    column_name));
  }

  if (!arrow_utils::isNumericType(column->type_id())) {
    return arrow::Status::TypeError(fmt::format(
        "Threshold state machine requires numeric type, but {} type "
        "provided",
        column->type()->ToString()));
  }

  auto value_result = column->GetScalar(row_id);
  ARROW_RETURN_NOT_OK(value_result.status());
  auto cast_result = value_result.ValueOrDie()->CastTo(arrow::float64());
  ARROW_RETURN_NOT_OK(cast_result.status());

  *value =
      std::static_pointer_cast<arrow::DoubleScalar>(cast_result.ValueOrDie())
          ->value;

  return arrow::Status::OK();
}

arrow::Status ThresholdState::getTimeAtRow(
    const std::shared_ptr<arrow::RecordBatch>& record_batch, int row_id,
    std::time_t* time) {
  std::string time_column_name;
  ARROW_RETURN_NOT_OK(
      metadata::getTimeColumnNameMetadata(record_batch, &time_column_name));

  auto time_result =
      record_batch->GetColumnByName(time_column_name)->GetScalar(row_id);

  ARROW_RETURN_NOT_OK(time_result.status());

  auto cast_result = time_result.ValueOrDie()->CastTo(
      arrow::timestamp(arrow::TimeUnit::SECOND));

  ARROW_RETURN_NOT_OK(cast_result.status());

  *time =
      std::static_pointer_cast<arrow::Int64Scalar>(cast_result.ValueOrDie())
          ->value;

  return arrow::Status::OK();
}

StateOK::StateOK(const std::shared_ptr<ThresholdStateMachine>& state_machine,
                 double current_threshold)
    : state_machine_(state_machine), current_threshold_(current_threshold) {}

StateOK::StateOK(const std::weak_ptr<ThresholdStateMachine>& state_machine,
                 double current_threshold)
    : state_machine_(state_machine), current_threshold_(current_threshold) {}

arrow::Status StateOK::addThresholdForRow(
    const std::shared_ptr<arrow::RecordBatch>& record_batch, int row_id,
    arrow::DoubleBuilder* threshold_column_builder) {
  double value;
  auto& options = state_machine_.lock()->getOptions();

  ARROW_RETURN_NOT_OK(getColumnValueAtRow(
      record_batch, options.watch_column_name, row_id, &value));

  ARROW_RETURN_NOT_OK(threshold_column_builder->Append(current_threshold_));

  if (value <= current_threshold_ &&
      value >= current_threshold_ * options.decrease_trigger_factor) {
    return arrow::Status::OK();
  }

  auto self = state_machine_.lock()->getState();
  std::time_t alert_start;
  ARROW_RETURN_NOT_OK(getTimeAtRow(record_batch, row_id, &alert_start));

  if (value > current_threshold_) {
    state_machine_.lock()->changeState(std::make_shared<StateIncrease>(
        state_machine_, current_threshold_, alert_start));

    return arrow::Status::OK();
  }

  if (value <= current_threshold_ * options.decrease_trigger_factor) {
    state_machine_.lock()->changeState(std::make_shared<StateDecrease>(
        state_machine_, current_threshold_, alert_start));

    return arrow::Status::OK();
  }

  return arrow::Status::OK();
}

StateIncrease::StateIncrease(
    const std::shared_ptr<ThresholdStateMachine>& state_machine,
    double current_threshold, std::time_t alert_start)
    : state_machine_(state_machine),
      current_threshold_(current_threshold),
      alert_start_(alert_start) {}

StateIncrease::StateIncrease(
    const std::weak_ptr<ThresholdStateMachine>& state_machine,
    double current_threshold, std::time_t alert_start)
    : state_machine_(state_machine),
      current_threshold_(current_threshold),
      alert_start_(alert_start) {}

arrow::Status StateIncrease::addThresholdForRow(
    const std::shared_ptr<arrow::RecordBatch>& record_batch, int row_id,
    arrow::DoubleBuilder* threshold_column_builder) {
  double value;
  auto& options = state_machine_.lock()->getOptions();

  ARROW_RETURN_NOT_OK(getColumnValueAtRow(
      record_batch, options.watch_column_name, row_id, &value));

  std::time_t row_time;
  ARROW_RETURN_NOT_OK(getTimeAtRow(record_batch, row_id, &row_time));

  if (value > current_threshold_ && row_time > alert_start_ &&
      row_time - alert_start_ > options.increase_after.count()) {
    auto self =
        state_machine_.lock()
            ->getState();  // need to save this to avoid freeing our memory

    std::shared_ptr<ThresholdState> new_state = std::make_shared<StateOK>(
        state_machine_,
        std::min(current_threshold_ * options.increase_scale_factor,
                 options.max_threshold));

    state_machine_.lock()->changeState(new_state);

    ARROW_RETURN_NOT_OK(
        new_state->addThresholdForRow(  // delegate work to the new state
            record_batch, row_id, threshold_column_builder));

    return arrow::Status::OK();
  }

  ARROW_RETURN_NOT_OK(threshold_column_builder->Append(current_threshold_));

  if (value <= current_threshold_) {
    auto self = state_machine_.lock()->getState();

    state_machine_.lock()->changeState(
        std::make_shared<StateOK>(state_machine_, current_threshold_));

    return arrow::Status::OK();
  }

  return arrow::Status::OK();
}

StateDecrease::StateDecrease(
    const std::shared_ptr<ThresholdStateMachine>& state_machine,
    double current_threshold, std::time_t decrease_start)
    : state_machine_(state_machine),
      current_threshold_(current_threshold),
      decrease_start_(decrease_start) {}

StateDecrease::StateDecrease(
    const std::weak_ptr<ThresholdStateMachine>& state_machine,
    double current_threshold, std::time_t decrease_start)
    : state_machine_(state_machine),
      current_threshold_(current_threshold),
      decrease_start_(decrease_start) {}

arrow::Status StateDecrease::addThresholdForRow(
    const std::shared_ptr<arrow::RecordBatch>& record_batch, int row_id,
    arrow::DoubleBuilder* threshold_column_builder) {
  double value;
  auto& options = state_machine_.lock()->getOptions();

  ARROW_RETURN_NOT_OK(getColumnValueAtRow(
      record_batch, options.watch_column_name, row_id, &value));

  std::time_t row_time;
  ARROW_RETURN_NOT_OK(getTimeAtRow(record_batch, row_id, &row_time));

  if (value <= current_threshold_ * options.decrease_trigger_factor &&
      row_time > decrease_start_ &&
      row_time - decrease_start_ > options.decrease_after.count()) {
    auto self =
        state_machine_.lock()
            ->getState();  // need to save this to avoid freeing our memory

    std::shared_ptr<ThresholdState> new_state = std::make_shared<StateOK>(
        state_machine_,
        std::max(current_threshold_ * options.decrease_scale_factor,
                 options.min_threshold));

    state_machine_.lock()->changeState(new_state);

    ARROW_RETURN_NOT_OK(
        new_state->addThresholdForRow(  // delegate work to the new state
            record_batch, row_id, threshold_column_builder));

    return arrow::Status::OK();
  }

  ARROW_RETURN_NOT_OK(threshold_column_builder->Append(current_threshold_));

  if (value > current_threshold_) {
    auto self = state_machine_.lock()->getState();

    state_machine_.lock()->changeState(std::make_shared<StateIncrease>(
        state_machine_, current_threshold_, row_time));

    return arrow::Status::OK();
  }

  if (value > current_threshold_ * options.decrease_trigger_factor) {
    auto self = state_machine_.lock()->getState();

    state_machine_.lock()->changeState(std::make_shared<StateOK>(
        state_machine_, current_threshold_ * options.decrease_scale_factor));

    return arrow::Status::OK();
  }

  return arrow::Status::OK();
}

}  // namespace internal

arrow::Status ThresholdStateMachine::handle(
    const std::shared_ptr<arrow::RecordBatch>& record_batch,
    arrow::RecordBatchVector* result) {
  if (state_ == nullptr) {
    return arrow::Status::Invalid("Threshold state is not set");
  }

  arrow::DoubleBuilder threshold_builder;
  for (int i = 0; i < record_batch->num_rows(); ++i) {
    ARROW_RETURN_NOT_OK(
        state_->addThresholdForRow(record_batch, i, &threshold_builder));
  }

  std::shared_ptr<arrow::Array> threshold_array;
  ARROW_RETURN_NOT_OK(threshold_builder.Finish(&threshold_array));

  result->push_back(record_batch);
  auto add_column_result = result->back()->AddColumn(
      record_batch->num_columns(),
      arrow::field(options_.threshold_column_name, arrow::float64()),
      threshold_array);

  ARROW_RETURN_NOT_OK(add_column_result.status());
  result->back() = add_column_result.ValueOrDie();

  ARROW_RETURN_NOT_OK(metadata::setColumnTypeMetadata(
      &result->back(), options_.threshold_column_name,
      options_.threshold_column_type));

  return arrow::Status::OK();
}

void ThresholdStateMachine::changeState(
    std::shared_ptr<internal::ThresholdState> new_threshold_state) {
  state_ = new_threshold_state;
}

const ThresholdStateMachine::Options& ThresholdStateMachine::getOptions()
    const {
  return options_;
}

std::shared_ptr<const internal::ThresholdState>
ThresholdStateMachine::getState() const {
  return state_;
}

std::shared_ptr<RecordBatchHandler>
ThresholdStateMachineFactory::createHandler() const {
  auto handler = std::make_shared<ThresholdStateMachine>(options_);

  handler->changeState(std::make_shared<internal::StateOK>(
      handler, options_.default_threshold));

  return handler;
}

}  // namespace stream_data_processor
