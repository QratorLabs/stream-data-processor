#include "node_base.h"

#include <spdlog/spdlog.h>

NodeBase::NodeBase(std::string name, const std::shared_ptr<uvw::Loop> &loop, TransportUtils::Subscriber&& subscriber)
    : name_(std::move(name))
    , logger_(spdlog::basic_logger_mt(name_, "logs/" + name_ + ".txt", true))
    , subscriber_(std::move(subscriber))
    , poller_(loop->resource<uvw::PollHandle>(subscriber_.subscriber_socket().getsockopt<int>(ZMQ_FD))) {
  spdlog::get(name_)->info("Node started");
}

zmq::message_t NodeBase::readMessage() {
  zmq::message_t message;
  auto recv_result = subscriber_.subscriber_socket().recv(message);
  if (!recv_result.has_value()) {
    spdlog::get(name_)->error("Error while receiving message, error code: {}", zmq_errno());
  }

  spdlog::get(name_)->info("Data received, size: {}", message.size());
  return message;
}
