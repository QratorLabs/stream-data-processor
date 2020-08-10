#pragma once

#include <memory>

#include "uvw.hpp"

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
  void configurePoller();

  zmq::message_t readMessage();

 private:
  TransportUtils::Subscriber subscriber_;
  std::shared_ptr<uvw::PollHandle> poller_;
};


