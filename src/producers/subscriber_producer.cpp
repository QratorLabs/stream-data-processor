#include <zmq.hpp>

#include "subscriber_producer.h"

SubscriberProducer::SubscriberProducer(std::shared_ptr<Node> node,
                                       TransportUtils::Subscriber &&subscriber,
                                       const std::shared_ptr<uvw::Loop>& loop)
                                       : Producer(std::move(node))
                                       , subscriber_(std::move(subscriber))
                                       , poller_(loop->resource<uvw::PollHandle>(
                                           subscriber_.subscriber_socket().getsockopt<int>(ZMQ_FD)
                                               )) {
  configurePoller();
}

void SubscriberProducer::start() {
  poller_->start(uvw::Flags<uvw::PollHandle::Event>::from<
      uvw::PollHandle::Event::READABLE, uvw::PollHandle::Event::DISCONNECT
  >());
}

void SubscriberProducer::configurePoller() {
  poller_->on<uvw::PollEvent>([this](const uvw::PollEvent& event, uvw::PollHandle& poller) {
    node_->log("Polled socket with events: " +
                std::to_string(subscriber_.subscriber_socket().getsockopt<int>(ZMQ_EVENTS)),
                spdlog::level::debug);
    while (subscriber_.subscriber_socket().getsockopt<int>(ZMQ_EVENTS) & ZMQ_POLLIN) {
      auto message = readMessage();
      if (!subscriber_.isReady()) {
        subscriber_.confirmConnection();
        node_->log("Connected to publisher", spdlog::level::info);
      } else if (message.to_string() == TransportUtils::END_MESSAGE) {
        node_->log("Closing connection with publisher", spdlog::level::info);
        stop();
        break;
      } else if (message.to_string() != TransportUtils::CONNECT_MESSAGE) {
        node_->handleData(static_cast<const char *>(message.data()), message.size());
      }
    }

    if (event.flags & uvw::PollHandle::Event::DISCONNECT) {
      node_->log("Closing connection with publisher", spdlog::level::info);
      stop();
    }
  });
}

void SubscriberProducer::stop() {
  poller_->close();
  subscriber_.subscriber_socket().close();
  subscriber_.synchronize_socket().close();
  node_->stop();
}

zmq::message_t SubscriberProducer::readMessage() {
  zmq::message_t message;
  try {
    message = TransportUtils::readMessage(subscriber_.subscriber_socket());
  } catch (const std::exception& e) {
    node_->log(e.what(), spdlog::level::err);
  }

  node_->log("Data received, size: " + std::to_string(message.size()), spdlog::level::info);
  return message;
}
