#pragma once

#include <memory>
#include <string>

#include <spdlog/sinks/basic_file_sink.h>

#include "uvw.hpp"

#include <zmq.hpp>

#include "utils/transport_utils.h"

class NodeBase {
 public:
  NodeBase(std::string name,
           const std::shared_ptr<uvw::Loop>& loop,
           TransportUtils::Subscriber&& subscriber);

 protected:
  zmq::message_t readMessage();

 protected:
  std::string name_;
  std::shared_ptr<spdlog::logger> logger_;
  TransportUtils::Subscriber subscriber_;
  std::shared_ptr<uvw::PollHandle> poller_;
};


