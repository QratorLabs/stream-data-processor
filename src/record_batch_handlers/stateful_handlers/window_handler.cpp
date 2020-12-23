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
      result.emplace_back();
      ARROW_ASSIGN_OR_RAISE(result.back(), emitWindow());
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

arrow::Result<std::shared_ptr<arrow::RecordBatch>>
WindowHandler::emitWindow() {
  ARROW_RETURN_NOT_OK(removeOldRecords());

  arrow::RecordBatchVector window_batches;
  for (auto& batch : buffered_record_batches_) {
    window_batches.push_back(batch);
  }

  std::shared_ptr<arrow::RecordBatch> window;
  ARROW_ASSIGN_OR_RAISE(
      window, convert_utils::concatenateRecordBatches(window_batches));

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
  if (record_batch->GetColumnByName(options_.every_column_name) == nullptr) {
    return arrow::Status::KeyError(
        fmt::format("Column {} not found", options_.every_column_name));
  }

  if (record_batch->GetColumnByName(options_.period_column_name) == nullptr) {
    return arrow::Status::KeyError(
        fmt::format("Column {} not found", options_.period_column_name));
  }

  time_utils::TimeUnit every_time_unit, period_time_unit;
  ARROW_ASSIGN_OR_RAISE(
      every_time_unit,
      getColumnTimeUnit(*record_batch, options_.every_column_name));
  ARROW_ASSIGN_OR_RAISE(
      period_time_unit,
      getColumnTimeUnit(*record_batch, options_.period_column_name));

  std::string time_column_name;
  ARROW_ASSIGN_OR_RAISE(time_column_name,
                        metadata::getTimeColumnNameMetadata(*record_batch));

  std::shared_ptr<arrow::RecordBatch> sorted_by_time;
  ARROW_ASSIGN_OR_RAISE(sorted_by_time, compute_utils::sortByColumn(
                                            time_column_name, record_batch));

  arrow::RecordBatchVector result;
  int64_t new_options_index;
  std::chrono::seconds duration_option;
  std::time_t new_options_ts;
  while (sorted_by_time->num_rows() > 0) {
    ARROW_ASSIGN_OR_RAISE(
        new_options_index,
        findNewWindowOptionsIndex(*sorted_by_time, every_time_unit,
                                  period_time_unit));

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

    ARROW_ASSIGN_OR_RAISE(
        duration_option,
        getDurationOption(*sorted_by_time, 0, options_.every_column_name,
                          every_time_unit));
    window_handler_->setEveryOption(duration_option, new_options_ts);
    ARROW_ASSIGN_OR_RAISE(
        duration_option,
        getDurationOption(*sorted_by_time, 0, options_.period_column_name,
                          period_time_unit));
    window_handler_->setPeriodOption(duration_option, new_options_ts);
  }

  return result;
}

arrow::Result<int64_t> DynamicWindowHandler::findNewWindowOptionsIndex(
    const arrow::RecordBatch& record_batch,
    time_utils::TimeUnit every_time_unit,
    time_utils::TimeUnit period_time_unit) const {
  int64_t new_options_row_index = 0;
  std::chrono::seconds current_every_value, current_period_value;
  while (new_options_row_index < record_batch.num_rows()) {
    ARROW_ASSIGN_OR_RAISE(
        current_every_value,
        getDurationOption(record_batch, new_options_row_index,
                          options_.every_column_name, every_time_unit));
    ARROW_ASSIGN_OR_RAISE(
        current_period_value,
        getDurationOption(record_batch, new_options_row_index,
                          options_.period_column_name, period_time_unit));

    if (current_every_value != window_handler_->getEveryOption() ||
        current_period_value != window_handler_->getPeriodOption()) {
      break;
    }

    ++new_options_row_index;
  }

  return new_options_row_index;
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

}  // namespace stream_data_processor
