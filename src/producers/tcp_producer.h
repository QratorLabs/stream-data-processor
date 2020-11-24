#pragma once

#include <uvw.hpp>

#include "producer.h"

#include "utils/transport_utils.h"

namespace stream_data_processor {

using transport_utils::IPv4Endpoint;

class TCPProducer : public Producer {
 public:
  TCPProducer(const std::shared_ptr<Node>& node,
              const IPv4Endpoint& listen_endpoint, uvw::Loop* loop,
              bool is_external);

  void start() override;
  void stop() override;

 private:
  void configureListener();
  void handleData(const char* data, size_t length);

 private:
  std::shared_ptr<uvw::TCPHandle> listener_;
  bool is_external_;
};

}  // namespace stream_data_processor
