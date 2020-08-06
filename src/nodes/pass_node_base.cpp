#include <chrono>
#include <utility>

#include <spdlog/spdlog.h>

#include "pass_node_base.h"

const std::chrono::duration<uint64_t, std::milli> PassNodeBase::RETRY_DELAY(100);

PassNodeBase::PassNodeBase(std::string name,
                           const std::shared_ptr<uvw::Loop>& loop,
                           TransportUtils::Subscriber&& subscriber,
                           TransportUtils::Publisher&& publisher)
                           : NodeBase(std::move(name), loop, std::move(subscriber))
                           , publisher_(std::move(publisher)) {

}

void PassNodeBase::startConnectingToTargets() {
  publisher_.startConnecting();
}

void PassNodeBase::sendData(const std::shared_ptr<arrow::Buffer> &data) {
  spdlog::get(name_)->info("Sending data of size: {}", data->size());
  zmq::message_t message(data->size());
  memcpy(message.data(), data->data(), data->size());
  auto send_result = publisher_.publisher_socket().send(message, zmq::send_flags::none);
  if (!send_result.has_value()) {
    spdlog::get(name_)->error("Error while sending data, error code: {}", zmq_errno());
  }
}
