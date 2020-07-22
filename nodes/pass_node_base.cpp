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
  spdlog::get(name_)->info("Sending data");
  for (auto &target : targets_) {
    target->write(reinterpret_cast<char *>(data->mutable_data()), data->size());
  }
}
