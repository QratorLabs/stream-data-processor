#include "window_handler.h"
#include "utils/utils.h"

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

}  // namespace stream_data_processor
