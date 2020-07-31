#pragma once

#include <arrow/api.h>

#include "node_base.h"

class PassNodeBase : public NodeBase {
 public:
  PassNodeBase(std::string name,
               const std::shared_ptr<uvw::Loop>& loop,
               const IPv4Endpoint& listen_endpoint,
               const std::vector<IPv4Endpoint>& target_endpoints);

 protected:
  void configureTarget(std::shared_ptr<uvw::TCPHandle> &target, const IPv4Endpoint &endpoint);
  void sendData(const std::shared_ptr<arrow::Buffer>& data);

 protected:
  static const std::chrono::duration<uint64_t, std::milli> RETRY_DELAY;

  std::vector<std::shared_ptr<uvw::TCPHandle>> targets_;
};


