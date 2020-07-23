#include "pass_node_base.h"

#include <utility>

#include <spdlog/spdlog.h>

PassNodeBase::PassNodeBase(std::string name,
                           const std::shared_ptr<uvw::Loop> &loop,
                           const IPv4Endpoint &listen_endpoint,
                           const std::vector<IPv4Endpoint> &target_endpoints)
                           : NodeBase(std::move(name), loop, listen_endpoint) {
  for (auto& target_endpoint : target_endpoints) {
    targets_.push_back(server_->loop().resource<uvw::TCPHandle>());
  }
}

void PassNodeBase::sendData(const std::shared_ptr<arrow::Buffer> &data) {
  std::shared_ptr<arrow::Buffer> terminated_buffer;
  auto terminate_status = Utils::terminate(data, &terminated_buffer);
  if (!terminate_status.ok()){
    spdlog::get(name_)->error(terminate_status.ToString());
    return;
  }

  spdlog::get(name_)->info("Sending data of size: {}", terminated_buffer->size());
  for (auto &target : targets_) {
    target->write(reinterpret_cast<char *>(terminated_buffer->mutable_data()), terminated_buffer->size());
  }
}
