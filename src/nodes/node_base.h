#pragma once

#include <memory>
#include <string>

#include <spdlog/sinks/basic_file_sink.h>

#include "uvw.hpp"

#include "utils/network_utils.h"

class NodeBase {
 public:
  NodeBase(std::string name,
           const std::shared_ptr<uvw::Loop>& loop,
           const IPv4Endpoint& listen_endpoint);

 protected:
  std::string name_;
  std::shared_ptr<spdlog::logger> logger_;
  std::shared_ptr<uvw::TCPHandle> server_;
};


