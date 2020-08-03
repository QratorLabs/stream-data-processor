#include <chrono>
#include <utility>

#include <spdlog/spdlog.h>

#include "pass_node_base.h"

const std::chrono::duration<uint64_t, std::milli> PassNodeBase::RETRY_DELAY(100);

PassNodeBase::PassNodeBase(std::string name,
                           const std::shared_ptr<uvw::Loop> &loop,
                           const IPv4Endpoint &listen_endpoint,
                           const std::vector<IPv4Endpoint> &target_endpoints)
                           : NodeBase(std::move(name), loop, listen_endpoint) {
  for (size_t i = 0; i < target_endpoints.size(); ++i) {
    spdlog::get(name_)->info("Configuring target {}:{}", target_endpoints[i].host, target_endpoints[i].port);
    targets_.push_back(server_->loop().resource<uvw::TCPHandle>());
    connect_timers_.push_back(server_->loop().resource<uvw::TimerHandle>());
    configureConnectTimer(i, target_endpoints[i]);
    configureTarget(i, target_endpoints[i]);
  }
}

void PassNodeBase::configureConnectTimer(size_t target_idx, const IPv4Endpoint &endpoint) {
  connect_timers_[target_idx]->on<uvw::TimerEvent>([this, target_idx, endpoint](const uvw::TimerEvent& event, uvw::TimerHandle& timer) {
    spdlog::get(name_)->info("Retrying connection to {}:{}", endpoint.host, endpoint.port);
    targets_[target_idx]->close();
    targets_[target_idx] = server_->loop().resource<uvw::TCPHandle>();
    configureTarget(target_idx, endpoint);
  });
}

void PassNodeBase::configureTarget(size_t target_idx, const IPv4Endpoint &endpoint) {
  targets_[target_idx]->once<uvw::ConnectEvent>([this, target_idx](const uvw::ConnectEvent& event, uvw::TCPHandle& target) {
    spdlog::get(name_)->info("Successfully connected to {}:{}", target.peer().ip, target.peer().port);
    connect_timers_[target_idx]->stop();
    connect_timers_[target_idx]->close();
  });

  targets_[target_idx]->on<uvw::ErrorEvent>([this, target_idx](const uvw::ErrorEvent& event, uvw::TCPHandle& target) {
    spdlog::get(name_)->error("Error code: {}. {}", event.code(), event.what());
    if (event.code() == -61) { // connection refused, try again later
      connect_timers_[target_idx]->start(RETRY_DELAY, std::chrono::duration<uint64_t, std::milli>(0));
    }
  });

  targets_[target_idx]->connect(endpoint.host, endpoint.port);
}

void PassNodeBase::sendData(const std::shared_ptr<arrow::Buffer> &data) {
  std::shared_ptr<arrow::Buffer> terminated_buffer;
  auto terminate_status = NetworkUtils::wrapMessage(data, &terminated_buffer);
  if (!terminate_status.ok()){
    spdlog::get(name_)->error(terminate_status.ToString());
    return;
  }

  spdlog::get(name_)->info("Sending data of size: {}", terminated_buffer->size());
  for (auto &target : targets_) {
    target->write(reinterpret_cast<char *>(terminated_buffer->mutable_data()), terminated_buffer->size());
  }
}
