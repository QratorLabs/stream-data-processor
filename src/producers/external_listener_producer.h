#pragma once

#include "uvw.hpp"

#include "producer.h"

#include "utils/transport_utils.h"

class ExternalListenerProducer : public Producer {
 public:
  ExternalListenerProducer(std::shared_ptr<Node> node,
                           const IPv4Endpoint &listen_endpoint,
                           const std::shared_ptr<uvw::Loop>& loop);

  void start() override;
  void stop() override;

 private:
  void configureListener();

 private:
  std::shared_ptr<uvw::TCPHandle> listener_;
};


