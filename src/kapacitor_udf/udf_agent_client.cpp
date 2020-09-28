#include "udf_agent_client.h"

void AgentClient::start() {
  agent_->start();
}

void AgentClient::stop() {
  agent_->stop();
}

AgentClient::AgentClient(std::shared_ptr<IUDFAgent> agent) : agent_(std::move(agent)) {

}
