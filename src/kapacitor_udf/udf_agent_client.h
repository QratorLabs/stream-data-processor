#pragma once

#include <memory>

#include <uvw.hpp>

#include "udf_agent.h"
#include "server/unix_socket_client.h"

class AgentClient : public UnixSocketClient {
 public:
  explicit AgentClient(std::shared_ptr<IUDFAgent> agent);

  void start() override;
  void stop() override;

 private:
  std::shared_ptr<IUDFAgent> agent_;
};
