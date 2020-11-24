#pragma once

#include <chrono>
#include <memory>
#include <queue>
#include <vector>

#include <arrow/api.h>

#include <uvw.hpp>

#include "consumer.h"
#include "utils/transport_utils.h"

namespace stream_data_processor {

using transport_utils::IPv4Endpoint;

class TCPConsumer : public Consumer {
 public:
  TCPConsumer(const std::vector<IPv4Endpoint>& target_endpoints,
              uvw::Loop* loop, bool is_external = false);

  void start() override;
  void consume(std::shared_ptr<arrow::Buffer> data) override;
  void stop() override;

 private:
  void configureConnectTimer(size_t target_idx, const IPv4Endpoint& endpoint);
  void configureTarget(size_t target_idx, const IPv4Endpoint& endpoint);
  void sendData(const std::shared_ptr<arrow::Buffer>& data);
  void flushBuffers();

 private:
  static const std::chrono::duration<uint64_t, std::milli> RETRY_DELAY;
  static const int CONNECTION_REFUSED_ERROR_CODE = -61;

  bool is_external_;
  std::vector<std::shared_ptr<uvw::TCPHandle>> targets_;
  std::vector<std::shared_ptr<uvw::TimerHandle>> connect_timers_;
  size_t connected_targets_{0};
  std::queue<std::shared_ptr<arrow::Buffer>> data_buffers_;
};

}  // namespace stream_data_processor
