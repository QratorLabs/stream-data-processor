#include "tcp_consumer.h"

namespace stream_data_processor {

using transport_utils::IPv4Endpoint;
using transport_utils::TransportUtils;

const std::chrono::duration<uint64_t, std::milli> TCPConsumer::RETRY_DELAY(
    100);

TCPConsumer::TCPConsumer(const std::vector<IPv4Endpoint>& target_endpoints,
                         uvw::Loop* loop, bool is_external)
    : is_external_(is_external) {
  for (size_t i = 0; i < target_endpoints.size(); ++i) {
    targets_.push_back(loop->resource<uvw::TCPHandle>());
    connect_timers_.push_back(loop->resource<uvw::TimerHandle>());
    configureConnectTimer(i, target_endpoints[i]);
    configureTarget(i, target_endpoints[i]);
  }
}

void TCPConsumer::configureConnectTimer(size_t target_idx,
                                        const IPv4Endpoint& endpoint) {
  connect_timers_[target_idx]->on<uvw::TimerEvent>(
      [this, target_idx, endpoint](const uvw::TimerEvent& event,
                                   uvw::TimerHandle& timer) {
        targets_[target_idx]->close();
        targets_[target_idx] = timer.loop().resource<uvw::TCPHandle>();
        configureTarget(target_idx, endpoint);
      });
}

void TCPConsumer::configureTarget(size_t target_idx,
                                  const IPv4Endpoint& endpoint) {
  targets_[target_idx]->once<uvw::ConnectEvent>(
      [this, target_idx](const uvw::ConnectEvent& event,
                         uvw::TCPHandle& target) {
        ++connected_targets_;
        connect_timers_[target_idx]->stop();
        connect_timers_[target_idx]->close();
      });

  targets_[target_idx]->on<uvw::ErrorEvent>(
      [this, target_idx](const uvw::ErrorEvent& event,
                         uvw::TCPHandle& target) {
        if (event.code() ==
            CONNECTION_REFUSED_ERROR_CODE) {  // connection refused, try again
                                              // later
          connect_timers_[target_idx]->start(
              RETRY_DELAY, std::chrono::duration<uint64_t, std::milli>(0));
        }
      });

  targets_[target_idx]->connect(endpoint.host, endpoint.port);
}

void TCPConsumer::sendData(const std::shared_ptr<arrow::Buffer>& data) {
  std::shared_ptr<arrow::Buffer> wrapped_buffer;
  if (is_external_) {
    wrapped_buffer = data;
  } else {
    auto wrapping_result = TransportUtils::wrapMessage(data);
    if (!wrapping_result.ok()) {
      throw std::runtime_error(wrapping_result.status().message());
    } else {
      wrapped_buffer = std::move(wrapping_result).ValueOrDie();
    }
  }

  for (size_t i = 0; i < targets_.size(); ++i) {
    if (!connect_timers_[i]->active()) {
      targets_[i]->write(
          reinterpret_cast<char*>(wrapped_buffer->mutable_data()),
          wrapped_buffer->size());
    }
  }
}

void TCPConsumer::start() {}

void TCPConsumer::consume(std::shared_ptr<arrow::Buffer> data) {
  data_buffers_.push(std::move(data));
  if (connected_targets_ == targets_.size()) {
    flushBuffers();
  }
}

void TCPConsumer::stop() {
  flushBuffers();
  for (size_t i = 0; i < targets_.size(); ++i) {
    connect_timers_[i]->close();
    targets_[i]->close();
  }
}

void TCPConsumer::flushBuffers() {
  while (!data_buffers_.empty()) {
    auto buffer = data_buffers_.front();
    sendData(buffer);
    data_buffers_.pop();
  }
}

}  // namespace stream_data_processor
