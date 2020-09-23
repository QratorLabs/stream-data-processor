#include "agent_client.h"

void AgentClient::start() {
  agent_->start();
}

void AgentClient::stop() {
  agent_->stop();
}

AgentClient::AgentClient(std::shared_ptr<IAgent> agent) : agent_(std::move(agent)) {

}

std::shared_ptr<UnixSocketClient> AgentClientFactory::createClient(const std::shared_ptr<uvw::PipeHandle> &pipe_handle) {
  std::shared_ptr<IAgent> agent = std::make_shared<Agent<uvw::PipeHandle, uv_pipe_t>>(pipe_handle, pipe_handle);
  return std::make_shared<AgentClient>(agent);
}
