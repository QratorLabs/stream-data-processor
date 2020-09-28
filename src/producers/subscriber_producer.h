#pragma once

#include <memory>

#include <uvw.hpp>

#include <zmq.hpp>

#include "producer.h"
#include "nodes/node.h"
#include "utils/transport_utils.h"

class SubscriberProducer : public Producer {
 public:
  SubscriberProducer(std::shared_ptr<Node> node,
                     TransportUtils::Subscriber &&subscriber,
                     const std::shared_ptr<uvw::Loop>& loop);

  void start() override;
  void stop() override;

 private:
  void configurePollers();

  void fetchSocketEvents();
  zmq::message_t readMessage();

  void confirmConnection();

 private:
  TransportUtils::Subscriber subscriber_;
  std::shared_ptr<uvw::PollHandle> poller_;
  std::shared_ptr<uvw::PollHandle> synchronize_poller_;
  bool ready_to_confirm_connection_{false};
};


