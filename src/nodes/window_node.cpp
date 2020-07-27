#include <utility>

#include <arrow/api.h>

#include <spdlog/spdlog.h>

#include "window_node.h"
#include "utils/utils.h"

WindowNode::WindowNode(std::string name,
                       const std::shared_ptr<uvw::Loop> &loop,
                       const IPv4Endpoint &listen_endpoint,
                       const std::vector<IPv4Endpoint> &target_endpoints,
                       uint64_t window_range, uint64_t window_period,
                       std::string ts_column_name)
                       : PassNodeBase(std::move(name), loop, listen_endpoint, target_endpoints)
                       , window_period_(window_period)
                       , ts_column_name_(std::move(ts_column_name))
                       , separation_idx_(std::vector<size_t>(
                           window_range / window_period + (window_range % window_period > 0 ? 1 : 0) - 1, 0
                           )) {
  configureServer();
  for (size_t i = 0; i < targets_.size(); ++i) {
    configureTarget(targets_[i]);
    targets_[i]->connect(target_endpoints[i].host, target_endpoints[i].port);
  }
}

void WindowNode::configureServer() {
  server_->once<uvw::ListenEvent>([this](const uvw::ListenEvent& event, uvw::TCPHandle& server) {
    spdlog::get(name_)->info("New client connection");

    auto client = server.loop().resource<uvw::TCPHandle>();

    client->on<uvw::DataEvent>([this](const uvw::DataEvent& event, uvw::TCPHandle& client) {
      spdlog::get(name_)->debug("Data received, size: {}", event.length);
      for (auto& data_part : NetworkUtils::splitMessage(event.data.get(), event.length)) {
        auto append_status = appendData(data_part.first, data_part.second);
        if (!append_status.ok()) {
          spdlog::get(name_)->error(append_status.ToString());
        }
      }
    });

    client->once<uvw::ErrorEvent>([this](const uvw::ErrorEvent& event, uvw::TCPHandle& client) {
      spdlog::get(name_)->error("Error: {}", event.what());
      send();
      stop();
      client.close();
    });

    client->once<uvw::EndEvent>([this](const uvw::EndEvent& event, uvw::TCPHandle& client) {
      spdlog::get(name_)->info("Closed connection with client");
      send();
      stop();
      client.close();
    });

    server.accept(*client);
    client->read();
  });
}

void WindowNode::configureTarget(const std::shared_ptr<uvw::TCPHandle> &target) {
  target->once<uvw::ConnectEvent>([this](const uvw::ConnectEvent& event, uvw::TCPHandle& target) {
    spdlog::get(name_)->info("Successfully connected to {}:{}", target.peer().ip, target.peer().port);
  });

  target->on<uvw::ErrorEvent>([this](const uvw::ErrorEvent& event, uvw::TCPHandle& target) {
    spdlog::get(name_)->error(event.what());
  });
}

arrow::Status WindowNode::appendData(char* data, size_t length) {
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
  server_->close();
  for (auto& target : targets_) {
    target->close();
  }
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
