#pragma once

#include <uvw.hpp>

#include "producer.h"

#include "utils/transport_utils.h"

class TCPProducer : public Producer {
 public:
  TCPProducer(std::shared_ptr<Node> node,
              const IPv4Endpoint &listen_endpoint,
              const std::shared_ptr<uvw::Loop> &loop,
              bool is_external = false);

  void start() override;
  void stop() override;

 private:
  void configureListener();
  void handleData(const char *data, size_t length);

 private:
  std::shared_ptr<uvw::TCPHandle> listener_;
  bool is_external_;
};


