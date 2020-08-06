#pragma once

#include <arrow/api.h>

#include <zmq.hpp>

#include "node_base.h"

class PassNodeBase : public NodeBase {
 public:
  PassNodeBase(std::string name,
               const std::shared_ptr<uvw::Loop>& loop,
               TransportUtils::Subscriber&& subscriber,
               TransportUtils::Publisher&& publisher);

 protected:
  void startConnectingToTargets();

  void sendData(const std::shared_ptr<arrow::Buffer>& data);

 protected:
  static const std::chrono::duration<uint64_t, std::milli> RETRY_DELAY;

  TransportUtils::Publisher publisher_;
};


