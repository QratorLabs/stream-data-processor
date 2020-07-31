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
    configureTarget(targets_[i], target_endpoints[i]);
  }
}

void PassNodeBase::configureTarget(std::shared_ptr<uvw::TCPHandle> &target, const IPv4Endpoint &endpoint) {
  auto& loop = target->loop();
  auto connect_timer = loop.resource<uvw::TimerHandle>();
  connect_timer->on<uvw::TimerEvent>([this, &loop, &target, endpoint](const uvw::TimerEvent& event, uvw::TimerHandle& timer) {
    spdlog::get(name_)->info("Retrying connection to {}:{}", endpoint.host, endpoint.port);
    target = loop.resource<uvw::TCPHandle>();
    configureTarget(target, endpoint);
  });

  target->once<uvw::ConnectEvent>([connect_timer, this](const uvw::ConnectEvent& event, uvw::TCPHandle& target) {
    spdlog::get(name_)->info("Successfully connected to {}:{}", target.peer().ip, target.peer().port);
    connect_timer->stop();
  });

  target->on<uvw::ErrorEvent>([connect_timer, this](const uvw::ErrorEvent& event, uvw::TCPHandle& target) {
    spdlog::get(name_)->error(event.what());
    connect_timer->start(RETRY_DELAY, std::chrono::duration<uint64_t, std::milli>(0));
  });

  target->connect(endpoint.host, endpoint.port);
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
