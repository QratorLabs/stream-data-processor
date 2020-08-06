#include <utility>

#include <arrow/api.h>

#include <spdlog/spdlog.h>

#include "window_node.h"
#include "utils/utils.h"

WindowNode::WindowNode(std::string name,
                       const std::shared_ptr<uvw::Loop> &loop,
                       TransportUtils::Subscriber&& subscriber,
                       TransportUtils::Publisher&& publisher,
                       uint64_t window_range, uint64_t window_period,
                       std::string ts_column_name)
                       : PassNodeBase(std::move(name), loop, std::move(subscriber), std::move(publisher))
                       , window_period_(window_period)
                       , ts_column_name_(std::move(ts_column_name))
                       , separation_idx_(std::vector<size_t>(
                           window_range / window_period + (window_range % window_period > 0 ? 1 : 0) - 1, 0
                           )) {
  configureServer();
}

void WindowNode::configureServer() {
  poller_->on<uvw::PollEvent>([this](const uvw::PollEvent& event, uvw::PollHandle& poller) {
    if (subscriber_.subscriber_socket().getsockopt<int>(ZMQ_EVENTS) & ZMQ_POLLIN) {
      auto message = readMessage();
      if (!subscriber_.isReady()) {
        spdlog::get(name_)->info("Connected to publisher");
        subscriber_.confirmConnection();
      } else if (message.to_string() != TransportUtils::CONNECT_MESSAGE) {
        auto append_status = appendData(static_cast<const char *>(message.data()), message.size());
      }
    }

    if (event.flags & uvw::PollHandle::Event::DISCONNECT) {
      spdlog::get(name_)->info("Closed connection with publisher");
      send();
      stop();
    }
  });

  poller_->start(uvw::Flags<uvw::PollHandle::Event>::from<uvw::PollHandle::Event::READABLE, uvw::PollHandle::Event::DISCONNECT>());
}

arrow::Status WindowNode::appendData(const char *data, size_t length) {
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
  while (max_ts - first_ts_in_current_batch_ >= window_period_) {
    size_t divide_index;
    ARROW_RETURN_NOT_OK(tsLowerBound(record_batch, [this](std::time_t ts) {
      return ts - first_ts_in_current_batch_ >= window_period_;
    }, divide_index));

    if (divide_index > 0) {
      auto current_slice = record_batch->Slice(0, divide_index);
      data_buffers_.emplace_back();
      ARROW_RETURN_NOT_OK(Serializer::serializeRecordBatches(record_batch->schema(),
                                                        {current_slice},
                                                        &data_buffers_.back()));
    }

    send();

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

arrow::Status WindowNode::tsLowerBound(const std::shared_ptr<arrow::RecordBatch> &record_batch,
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

void WindowNode::send() {
  std::shared_ptr<arrow::Buffer> window_data;
  auto window_status = buildWindow(window_data);
  if (!window_status.ok()) {
    spdlog::get(name_)->debug(window_status.ToString());
    return;
  }

  sendData(window_data);
  removeOldBuffers();
}

void WindowNode::stop() {
  spdlog::get(name_)->info("Stopping node");
  poller_->close();
  subscriber_.subscriber_socket().close();
  subscriber_.synchronize_socket().close();
  publisher_.publisher_socket().close();
  publisher_.synchronize_socket().close();
}

arrow::Status WindowNode::buildWindow(std::shared_ptr<arrow::Buffer> &window_data) {
  arrow::RecordBatchVector window_vector;
  for (auto& buffer : data_buffers_) {
    arrow::RecordBatchVector record_batch_vector;
    ARROW_RETURN_NOT_OK(Serializer::deserializeRecordBatches(buffer, &record_batch_vector));
    for (auto& record_batch : record_batch_vector) {
      window_vector.push_back(record_batch);
    }
  }

  std::shared_ptr<arrow::RecordBatch> record_batch;
  ARROW_RETURN_NOT_OK(DataConverter::concatenateRecordBatches(window_vector, &record_batch));
  ARROW_RETURN_NOT_OK(Serializer::serializeRecordBatches(record_batch->schema(), {record_batch}, &window_data));
  return arrow::Status::OK();
}

void WindowNode::removeOldBuffers() {
  size_t shift = separation_idx_.front();
  data_buffers_.erase(data_buffers_.begin(), data_buffers_.begin() + shift);

  for (size_t i = 0; i < separation_idx_.size() - 1; ++i) {
    separation_idx_[i] = separation_idx_[i + 1] - shift;
  }

  separation_idx_.back() = data_buffers_.size();
}
