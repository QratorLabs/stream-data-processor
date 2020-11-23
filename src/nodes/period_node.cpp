#include <utility>

#include <spdlog/spdlog.h>

#include "period_node.h"
#include "utils/utils.h"

namespace stream_data_processor {

void PeriodNode::handleData(const char* data, size_t length) {
  auto append_result = appendData(data, length);
  if (!append_result.ok()) {
    spdlog::get(name_)->error(append_result.message());
  }
}

arrow::Status PeriodNode::appendData(const char* data, size_t length) {
  arrow::Buffer data_buffer(reinterpret_cast<const uint8_t*>(data), length);
  arrow::RecordBatchVector record_batches;
  ARROW_ASSIGN_OR_RAISE(
      record_batches, serialize_utils::deserializeRecordBatches(data_buffer));

  std::shared_ptr<arrow::RecordBatch> record_batch;
  ARROW_ASSIGN_OR_RAISE(
      record_batch, convert_utils::concatenateRecordBatches(record_batches));
  ARROW_ASSIGN_OR_RAISE(record_batch, compute_utils::sortByColumn(
                                          time_column_name_, record_batch));

  if (first_ts_in_current_batch_ == 0) {
    std::shared_ptr<arrow::Scalar> min_ts;
    ARROW_ASSIGN_OR_RAISE(
        min_ts,
        record_batch->GetColumnByName(time_column_name_)->GetScalar(0));

    first_ts_in_current_batch_ =
        std::static_pointer_cast<arrow::Int64Scalar>(min_ts)->value;
  }

  std::shared_ptr<arrow::Scalar> max_ts_scalar;
  ARROW_ASSIGN_OR_RAISE(max_ts_scalar,
                        record_batch->GetColumnByName(time_column_name_)
                            ->GetScalar(record_batch->num_rows() - 1));

  std::time_t max_ts(
      std::static_pointer_cast<arrow::Int64Scalar>(max_ts_scalar)->value);

  while (max_ts - first_ts_in_current_batch_ >= period_) {
    size_t divide_index;
    ARROW_RETURN_NOT_OK(tsLowerBound(
        *record_batch,
        [this](std::time_t ts) {
          return ts - first_ts_in_current_batch_ >= period_;
        },
        divide_index));

    if (divide_index > 0) {
      auto current_slice = record_batch->Slice(0, divide_index);
      std::vector<std::shared_ptr<arrow::Buffer>> buffers;

      ARROW_ASSIGN_OR_RAISE(
          buffers, serialize_utils::serializeRecordBatches({current_slice}));

      for (auto& buffer : buffers) { data_buffers_.push_back(buffer); }
    }

    pass();

    std::shared_ptr<arrow::Scalar> ts;
    ARROW_ASSIGN_OR_RAISE(ts, record_batch->GetColumnByName(time_column_name_)
                                  ->GetScalar(divide_index));

    first_ts_in_current_batch_ =
        std::static_pointer_cast<arrow::Int64Scalar>(ts)->value;

    record_batch = record_batch->Slice(divide_index);
  }

  std::vector<std::shared_ptr<arrow::Buffer>> buffers;

  ARROW_ASSIGN_OR_RAISE(
      buffers, serialize_utils::serializeRecordBatches({record_batch}));

  for (auto& buffer : buffers) { data_buffers_.push_back(buffer); }

  return arrow::Status::OK();
}

arrow::Status PeriodNode::tsLowerBound(
    const arrow::RecordBatch& record_batch,
    const std::function<bool(std::time_t)>& pred, size_t& lower_bound) {
  size_t left_bound = 0;
  size_t right_bound = record_batch.num_rows();
  while (left_bound != right_bound - 1) {
    auto middle = (left_bound + right_bound) / 2;

    std::shared_ptr<arrow::Scalar> ts_scalar;
    ARROW_ASSIGN_OR_RAISE(
        ts_scalar,
        record_batch.GetColumnByName(time_column_name_)->GetScalar(middle));

    std::time_t ts(
        std::static_pointer_cast<arrow::Int64Scalar>(ts_scalar)->value);

    if (pred(ts)) {
      right_bound = middle;
    } else {
      left_bound = middle;
    }
  }

  std::shared_ptr<arrow::Scalar> ts_scalar;
  ARROW_ASSIGN_OR_RAISE(
      ts_scalar,
      record_batch.GetColumnByName(time_column_name_)->GetScalar(left_bound));

  std::time_t ts(
      std::static_pointer_cast<arrow::Int64Scalar>(ts_scalar)->value);

  if (pred(ts)) {
    lower_bound = left_bound;
  } else {
    lower_bound = right_bound;
  }

  return arrow::Status::OK();
}

void PeriodNode::pass() {
  auto period_data = period_handler_->handle(data_buffers_);
  if (!period_data.ok()) {
    spdlog::get(name_)->debug(period_data.status().message());
    return;
  }

  passData(period_data.ValueOrDie());
  removeOldBuffers();
}

void PeriodNode::start() { spdlog::get(name_)->info("Node started"); }

void PeriodNode::stop() {
  pass();
  spdlog::get(name_)->info("Stopping node");
  for (auto& consumer : consumers_) { consumer->stop(); }
}

void PeriodNode::removeOldBuffers() {
  size_t shift = separation_idx_.front();
  data_buffers_.erase(data_buffers_.begin(), data_buffers_.begin() + shift);

  for (size_t i = 0; i < separation_idx_.size() - 1; ++i) {
    separation_idx_[i] = separation_idx_[i + 1] - shift;
  }

  separation_idx_.back() = data_buffers_.size();
}

}  // namespace stream_data_processor
