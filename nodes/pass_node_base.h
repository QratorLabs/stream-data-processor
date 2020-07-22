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
  void sendData(const std::shared_ptr<arrow::Buffer>& data);

 protected:
  std::vector<std::shared_ptr<uvw::TCPHandle>> targets_;
};


