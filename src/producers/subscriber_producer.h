#pragma once

#include <memory>

#include <uvw.hpp>

#include <zmq.hpp>

#include "nodes/node.h"
#include "producer.h"
#include "utils/transport_utils.h"

namespace stream_data_processor {

using transport_utils::TransportUtils;

class SubscriberProducer : public Producer {
 public:
  template <typename SubscriberType>
  SubscriberProducer(const std::shared_ptr<Node>& node,
                     SubscriberType&& subscriber, uvw::Loop* loop)
      : Producer(node),
        subscriber_(std::forward<SubscriberType>(subscriber)),
        poller_(loop->resource<uvw::PollHandle>(
            subscriber_.subscriber_socket().getsockopt<int>(ZMQ_FD))),
        synchronize_poller_(loop->resource<uvw::PollHandle>(
            subscriber_.synchronize_socket().getsockopt<int>(ZMQ_FD))) {
    configurePollers();
  }

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

}  // namespace stream_data_processor
