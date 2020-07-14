#pragma once

#include <chrono>
#include <memory>
#include <vector>

#include <arrow/api.h>

#include "uvw.hpp"

#include "data_handlers/data_handler.h"

#include "utils.h"

 class EvalNode {
 public:
  EvalNode(std::shared_ptr<uvw::Loop> loop,
           std::shared_ptr<DataHandler> data_handler,
           const IPv4Endpoint& listen_endpoint,
           const std::vector<IPv4Endpoint>& target_endpoints);

 private:
  void configureServer(const IPv4Endpoint& endpoint);
  void addTarget(const IPv4Endpoint& endpoint);

  void sendData();

  void stop();

 private:
  std::shared_ptr<uvw::Loop> loop_;
  std::shared_ptr<DataHandler> data_handler_;
  std::shared_ptr<uvw::TCPHandle> server_;
  std::shared_ptr<uvw::TimerHandle> timer_;
  std::shared_ptr<arrow::BufferBuilder> buffer_builder_;
  std::vector<std::shared_ptr<uvw::TCPHandle>> targets_;

  static const std::chrono::duration<uint64_t> SILENCE_TIMEOUT;
};


