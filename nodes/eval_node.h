#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "uvw.hpp"

#include "pass_node_base.h"
#include "data_handlers/data_handler.h"

class EvalNode : public PassNodeBase {
 public:
  EvalNode(std::string name,
      const std::shared_ptr<uvw::Loop>& loop,
      const IPv4Endpoint& listen_endpoint,
      const std::vector<IPv4Endpoint>& target_endpoints,
      std::shared_ptr<DataHandler> data_handler);

 private:
  void configureServer();
  void configureTarget(const std::shared_ptr<uvw::TCPHandle>& target);

  arrow::Status processData(std::shared_ptr<arrow::Buffer>& processed_data);
  void send();
  void stop();

 private:
  std::shared_ptr<DataHandler> data_handler_;
  std::shared_ptr<uvw::TimerHandle> timer_;
  std::shared_ptr<arrow::BufferBuilder> buffer_builder_;

  static const std::chrono::duration<uint64_t> SILENCE_TIMEOUT;
};


