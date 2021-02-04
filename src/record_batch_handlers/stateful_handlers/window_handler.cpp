#include <spdlog/spdlog.h>

#include "metadata/time_metadata.h"
#include "utils/utils.h"
#include "window_handler.h"

namespace stream_data_processor {

arrow::Result<arrow::RecordBatchVector> WindowHandler::handle(
    const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  std::string time_column_name;
  ARROW_ASSIGN_OR_RAISE(time_column_name,
                        metadata::getTimeColumnNameMetadata(*record_batch));

  std::shared_ptr<arrow::RecordBatch> sorted_record_batch;
  ARROW_ASSIGN_OR_RAISE(
      sorted_record_batch,
      compute_utils::sortByColumn(time_column_name, record_batch));

  if (next_emit_ == 0) {
    std::shared_ptr<arrow::Scalar> min_ts;
    ARROW_ASSIGN_OR_RAISE(
        min_ts, arrow_utils::castTimestampScalar(
                    sorted_record_batch->GetColumnByName(time_column_name)
                        ->GetScalar(0),
                    arrow::TimeUnit::SECOND));

    next_emit_ = std::static_pointer_cast<arrow::Int64Scalar>(min_ts)->value;
    if (options_.fill_period) {
      next_emit_ += options_.period.count();
    } else {
      next_emit_ += options_.every.count();
    }
  }

  std::shared_ptr<arrow::Scalar> max_ts_scalar;
  ARROW_ASSIGN_OR_RAISE(
      max_ts_scalar,
      arrow_utils::castTimestampScalar(
          sorted_record_batch->GetColumnByName(time_column_name)
              ->GetScalar(sorted_record_batch->num_rows() - 1),
          arrow::TimeUnit::SECOND));

  std::time_t max_ts(
      std::static_pointer_cast<arrow::Int64Scalar>(max_ts_scalar)->value);

  arrow::RecordBatchVector result;
  while (max_ts >= next_emit_) {
    size_t divide_index;
    ARROW_ASSIGN_OR_RAISE(
        divide_index, tsLowerBound(
                          *sorted_record_batch,
                          [this](std::time_t ts) { return ts >= next_emit_; },
                          time_column_name));

    if (divide_index > 0) {
      buffered_record_batches_.push_back(
          sorted_record_batch->Slice(0, divide_index));
    }

    if (!buffered_record_batches_.empty()) {
      arrow::RecordBatchVector window;
      ARROW_ASSIGN_OR_RAISE(window, emitWindow());
      convert_utils::append(std::move(window), result);
    }

    emitted_first_ = true;
    next_emit_ += options_.every.count();
    ARROW_RETURN_NOT_OK(removeOldRecords());

    sorted_record_batch = sorted_record_batch->Slice(divide_index);
  }

  buffered_record_batches_.push_back(sorted_record_batch);

  return result;
}

arrow::Result<size_t> WindowHandler::tsLowerBound(
    const arrow::RecordBatch& record_batch,
    const std::function<bool(std::time_t)>& pred,
    const std::string& time_column_name) {
  size_t left_bound = 0;
  size_t right_bound = record_batch.num_rows();
  while (left_bound != right_bound - 1) {
    auto middle = (left_bound + right_bound) / 2;

    std::shared_ptr<arrow::Scalar> ts_scalar;
    ARROW_ASSIGN_OR_RAISE(
        ts_scalar,
        arrow_utils::castTimestampScalar(
            record_batch.GetColumnByName(time_column_name)->GetScalar(middle),
            arrow::TimeUnit::SECOND));

    std::time_t ts(
        std::static_pointer_cast<arrow::Int64Scalar>(ts_scalar)->value);

    if (pred(ts)) {
      right_bound = middle;
    } else {
      left_bound = middle;
    }
  }

  std::shared_ptr<arrow::Scalar> ts_scalar;
  ARROW_ASSIGN_OR_RAISE(ts_scalar,
                        arrow_utils::castTimestampScalar(
                            record_batch.GetColumnByName(time_column_name)
                                ->GetScalar(left_bound),
                            arrow::TimeUnit::SECOND));

  std::time_t ts(
      std::static_pointer_cast<arrow::Int64Scalar>(ts_scalar)->value);

  if (pred(ts)) {
    return left_bound;
  } else {
    return right_bound;
  }
}

arrow::Result<arrow::RecordBatchVector> WindowHandler::emitWindow() {
  ARROW_RETURN_NOT_OK(removeOldRecords());

  arrow::RecordBatchVector window;
  if (buffered_record_batches_.empty()) {
    return window;
  }

  auto current_schema = buffered_record_batches_.front()->schema();
  arrow::RecordBatchVector window_batches;
  for (auto& batch : buffered_record_batches_) {
    if (!current_schema->Equals(batch->schema(), false)) {
      window.emplace_back();

      ARROW_ASSIGN_OR_RAISE(
          window.back(),
          convert_utils::concatenateRecordBatches(window_batches));

      window_batches.clear();
      current_schema = batch->schema();
    }

    window_batches.push_back(batch);
  }

  window.emplace_back();
  ARROW_ASSIGN_OR_RAISE(
      window.back(), convert_utils::concatenateRecordBatches(window_batches));

  return window;
}

arrow::Status WindowHandler::removeOldRecords() {
  while (true) {
    if (buffered_record_batches_.empty()) {
      return arrow::Status::OK();
    }

    auto& oldest_batch = buffered_record_batches_.front();
    std::string time_column_name;
    ARROW_ASSIGN_OR_RAISE(time_column_name,
                          metadata::getTimeColumnNameMetadata(*oldest_batch));

    size_t divide_index;
    ARROW_ASSIGN_OR_RAISE(
        divide_index, tsLowerBound(
                          *oldest_batch,
                          [this](std::time_t ts) {
                            return ts >= next_emit_ - options_.period.count();
                          },
                          time_column_name));

    if (divide_index < oldest_batch->num_rows()) {
      oldest_batch = oldest_batch->Slice(divide_index);
      return arrow::Status::OK();
    } else {
      buffered_record_batches_.pop_front();
    }
  }
}

arrow::Result<arrow::RecordBatchVector> DynamicWindowHandler::handle(
    const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  bool is_new_period_possible =
      options_.period_column_name.has_value() &&
      (record_batch->GetColumnByName(options_.period_column_name.value()) !=
       nullptr);

  bool is_new_every_possible =
      options_.every_column_name.has_value() &&
      (record_batch->GetColumnByName(options_.every_column_name.value()) !=
       nullptr);

  int64_t new_period_index, new_every_index;
  time_utils::TimeUnit period_time_unit, every_time_unit;
  if (is_new_period_possible) {
    ARROW_ASSIGN_OR_RAISE(
        period_time_unit,
        getColumnTimeUnit(*record_batch,
                          options_.period_column_name.value()));
    new_period_index = 0;
  } else {
    new_period_index = record_batch->num_rows();
  }
  if (is_new_every_possible) {
    ARROW_ASSIGN_OR_RAISE(
        every_time_unit,
        getColumnTimeUnit(*record_batch, options_.every_column_name.value()));
    new_every_index = 0;
  } else {
    new_every_index = record_batch->num_rows();
  }

  std::string time_column_name;
  ARROW_ASSIGN_OR_RAISE(time_column_name,
                        metadata::getTimeColumnNameMetadata(*record_batch));

  std::shared_ptr<arrow::RecordBatch> sorted_by_time;
  ARROW_ASSIGN_OR_RAISE(sorted_by_time, compute_utils::sortByColumn(
                                            time_column_name, record_batch));

  arrow::RecordBatchVector result;
  std::chrono::seconds duration_option;
  std::time_t new_options_ts;
  while (sorted_by_time->num_rows() > 0) {
    if (is_new_period_possible && new_period_index == 0) {
      ARROW_ASSIGN_OR_RAISE(
          new_period_index,
          findNewWindowOptionIndex(
              *sorted_by_time, window_handler_->getPeriodOption(),
              options_.period_column_name.value(), period_time_unit));
    }

    if (is_new_every_possible && new_every_index == 0) {
      ARROW_ASSIGN_OR_RAISE(
          new_every_index,
          findNewWindowOptionIndex(
              *sorted_by_time, window_handler_->getEveryOption(),
              options_.every_column_name.value(), every_time_unit));
    }

    auto new_options_index = std::min(new_every_index, new_period_index);
    if (new_options_index > 0) {
      arrow::RecordBatchVector intermediate_result;
      ARROW_ASSIGN_OR_RAISE(intermediate_result,
                            window_handler_->handle(
                                sorted_by_time->Slice(0, new_options_index)));

      convert_utils::append(std::move(intermediate_result), result);
    }

    sorted_by_time = sorted_by_time->Slice(new_options_index);
    if (sorted_by_time->num_rows() == 0) {
      break;
    }

    std::shared_ptr<arrow::Scalar> new_options_ts_scalar;
    ARROW_ASSIGN_OR_RAISE(
        new_options_ts_scalar,
        arrow_utils::castTimestampScalar(
            sorted_by_time->GetColumnByName(time_column_name)->GetScalar(0),
            arrow::TimeUnit::SECOND));
    new_options_ts =
        std::static_pointer_cast<arrow::Int64Scalar>(new_options_ts_scalar)
            ->value;

    if (is_new_period_possible && new_options_index == new_period_index) {
      ARROW_ASSIGN_OR_RAISE(
          duration_option,
          getDurationOption(*sorted_by_time, 0,
                            options_.period_column_name.value(),
                            period_time_unit));
      window_handler_->setPeriodOption(duration_option, new_options_ts);
    }

    if (is_new_every_possible && new_options_index == new_every_index) {
      ARROW_ASSIGN_OR_RAISE(
          duration_option,
          getDurationOption(*sorted_by_time, 0,
                            options_.every_column_name.value(),
                            every_time_unit));
      window_handler_->setEveryOption(duration_option, new_options_ts);
    }

    new_period_index -= new_options_index;
    new_every_index -= new_options_index;
  }

  return result;
}

arrow::Result<int64_t> DynamicWindowHandler::findNewWindowOptionIndex(
    const arrow::RecordBatch& record_batch,
    const std::chrono::seconds& current_option_value,
    const std::string& option_column_name,
    time_utils::TimeUnit option_time_unit) const {
  int64_t new_option_row_index = 0;
  std::chrono::seconds new_option_value;
  while (new_option_row_index < record_batch.num_rows()) {
    ARROW_ASSIGN_OR_RAISE(
        new_option_value,
        getDurationOption(record_batch, new_option_row_index,
                          option_column_name, option_time_unit));

    if (new_option_value != current_option_value) {
      break;
    }

    ++new_option_row_index;
  }

  return new_option_row_index;
}

arrow::Result<std::chrono::seconds> DynamicWindowHandler::getDurationOption(
    const arrow::RecordBatch& record_batch, int64_t row,
    const std::string& column_name, time_utils::TimeUnit time_unit) {
  auto column = record_batch.GetColumnByName(column_name);
  std::shared_ptr<arrow::Scalar> duration_scalar;
  ARROW_ASSIGN_OR_RAISE(duration_scalar, column->GetScalar(row));

  auto duration =
      std::static_pointer_cast<arrow::Int64Scalar>(duration_scalar)->value;

  int64_t seconds;
  ARROW_ASSIGN_OR_RAISE(seconds, time_utils::convertTime(duration, time_unit,
                                                         time_utils::SECOND));

  return std::chrono::seconds{seconds};
}

arrow::Result<time_utils::TimeUnit> DynamicWindowHandler::getColumnTimeUnit(
    const arrow::RecordBatch& record_batch, const std::string& column_name) {
  auto arrow_type = record_batch.GetColumnByName(column_name)->type();
  if (arrow_type->id() == arrow::Type::DURATION) {
    return time_utils::mapArrowTimeUnit(
        std::static_pointer_cast<arrow::DurationType>(arrow_type)->unit());
  } else if (arrow_type->id() == arrow::Type::TIMESTAMP) {
    return time_utils::mapArrowTimeUnit(
        std::static_pointer_cast<arrow::TimestampType>(arrow_type)->unit());
  }

  return metadata::getTimeUnitMetadata(record_batch, column_name);
}

std::shared_ptr<RecordBatchHandler>
DynamicWindowHandlerFactory::createHandler() const {
  return std::make_shared<DynamicWindowHandler>(
      std::make_shared<WindowHandler>(window_options_),
      dynamic_window_options_);
}

}  // namespace stream_data_processor
