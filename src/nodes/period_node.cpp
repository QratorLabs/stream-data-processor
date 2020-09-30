#include <utility>

#include <spdlog/spdlog.h>

#include "period_node.h"
#include "utils/utils.h"

void PeriodNode::handleData(const char *data, size_t length) {
  auto append_result = appendData(data, length);
  if (!append_result.ok()) {
    spdlog::get(name_)->error(append_result.message());
  }
}

arrow::Status PeriodNode::appendData(const char *data, size_t length) {
  auto data_buffer = std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t *>(data), length);
  arrow::RecordBatchVector record_batches;
  ARROW_RETURN_NOT_OK(Serializer::deserializeRecordBatches(data_buffer, &record_batches));

  std::shared_ptr<arrow::RecordBatch> record_batch;
  ARROW_RETURN_NOT_OK(DataConverter::concatenateRecordBatches(record_batches, &record_batch));
  ARROW_RETURN_NOT_OK(ComputeUtils::sortByColumn(ts_column_name_, record_batch, &record_batch));

  if (first_ts_in_current_batch_ == 0) {
    auto min_ts_result = record_batch->GetColumnByName(ts_column_name_)->GetScalar(0);
    if (!min_ts_result.ok()) {
      return min_ts_result.status();
    }

    first_ts_in_current_batch_ = std::static_pointer_cast<arrow::Int64Scalar>(min_ts_result.ValueOrDie())->value;
  }

  auto max_ts_result = record_batch->GetColumnByName(ts_column_name_)->GetScalar(record_batch->num_rows() - 1);
  if (!max_ts_result.ok()) {
    return max_ts_result.status();
  }

  std::time_t max_ts(std::static_pointer_cast<arrow::Int64Scalar>(max_ts_result.ValueOrDie())->value);
  while (max_ts - first_ts_in_current_batch_ >= period_) {
    size_t divide_index;
    ARROW_RETURN_NOT_OK(tsLowerBound(record_batch, [this](std::time_t ts) {
      return ts - first_ts_in_current_batch_ >= period_;
    }, divide_index));

    if (divide_index > 0) {
      auto current_slice = record_batch->Slice(0, divide_index);
      data_buffers_.emplace_back();
      ARROW_RETURN_NOT_OK(Serializer::serializeRecordBatches(record_batch->schema(),
                                                        {current_slice},
                                                        &data_buffers_.back()));
    }

    pass();

    auto ts_result = record_batch->GetColumnByName(ts_column_name_)->GetScalar(divide_index);
    if (!ts_result.ok()) {
      return ts_result.status();
    }

    first_ts_in_current_batch_ = std::static_pointer_cast<arrow::Int64Scalar>(ts_result.ValueOrDie())->value;
    record_batch = record_batch->Slice(divide_index);
  }

  data_buffers_.emplace_back();
  ARROW_RETURN_NOT_OK(Serializer::serializeRecordBatches(record_batch->schema(),
      {record_batch},
      &data_buffers_.back()));
  return arrow::Status::OK();
}

arrow::Status PeriodNode::tsLowerBound(const std::shared_ptr<arrow::RecordBatch> &record_batch,
                                       const std::function<bool(std::time_t)>& pred,
                                       size_t &lower_bound) {
  size_t left_bound = 0;
  size_t right_bound = record_batch->num_rows();
  while (left_bound != right_bound - 1) {
    auto middle = (left_bound + right_bound) / 2;
    auto ts_result = record_batch->GetColumnByName(ts_column_name_)->GetScalar(middle);
    if (!ts_result.ok()) {
      return ts_result.status();
    }

    std::time_t ts(std::static_pointer_cast<arrow::Int64Scalar>(ts_result.ValueOrDie())->value);
    if (pred(ts)) {
      right_bound = middle;
    } else {
      left_bound = middle;
    }
  }

  auto ts_result = record_batch->GetColumnByName(ts_column_name_)->GetScalar(left_bound);
  if (!ts_result.ok()) {
    return ts_result.status();
  }

  std::time_t ts(std::static_pointer_cast<arrow::Int64Scalar>(ts_result.ValueOrDie())->value);
  if (pred(ts)) {
    lower_bound = left_bound;
  } else {
    lower_bound = right_bound;
  }

  return arrow::Status::OK();
}

void PeriodNode::pass() {
  std::shared_ptr<arrow::Buffer> period_data;
  auto handle_status = period_handler_->handle(data_buffers_, &period_data);
  if (!handle_status.ok()) {
    spdlog::get(name_)->debug(handle_status.ToString());
    return;
  }

  passData(period_data);
  removeOldBuffers();
}

void PeriodNode::start() {
  spdlog::get(name_)->info("Node started");
}

void PeriodNode::stop() {
  pass();
  spdlog::get(name_)->info("Stopping node");
  for (auto& consumer : consumers_) {
    consumer->stop();
  }
}

void PeriodNode::removeOldBuffers() {
  size_t shift = separation_idx_.front();
  data_buffers_.erase(data_buffers_.begin(), data_buffers_.begin() + shift);

  for (size_t i = 0; i < separation_idx_.size() - 1; ++i) {
    separation_idx_[i] = separation_idx_[i + 1] - shift;
  }

  separation_idx_.back() = data_buffers_.size();
}
