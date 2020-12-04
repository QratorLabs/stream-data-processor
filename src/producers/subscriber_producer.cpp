#include <zmq.hpp>

#include "subscriber_producer.h"

namespace stream_data_processor {

using transport_utils::TransportUtils;

void SubscriberProducer::start() {
  fetchSocketEvents();
  poller_->start(uvw::Flags<uvw::PollHandle::Event>::from<
                 uvw::PollHandle::Event::READABLE,
                 uvw::PollHandle::Event::DISCONNECT>());
  synchronize_poller_->start(uvw::Flags<uvw::PollHandle::Event>::from<
                             uvw::PollHandle::Event::WRITABLE>());
}

void SubscriberProducer::configurePollers() {
  poller_->on<uvw::PollEvent>([this](const uvw::PollEvent& event,
                                     uvw::PollHandle& poller) {
    log("Polled socket with events: " +
            std::to_string(
                subscriber_.subscriber_socket().getsockopt<int>(ZMQ_EVENTS)),
        spdlog::level::debug);
    while (subscriber_.subscriber_socket().getsockopt<int>(ZMQ_EVENTS) &
           ZMQ_POLLIN) {
      auto message = readMessage();
      if (message.to_string() == TransportUtils::END_MESSAGE) {
        log("Closing connection with publisher", spdlog::level::info);
        stop();
        break;
      } else if (!subscriber_.isReady()) {
        subscriber_.prepareForListening();
        if (ready_to_confirm_connection_) {
          confirmConnection();
        }

        log("Connected to publisher", spdlog::level::info);
      } else if (message.to_string() != TransportUtils::CONNECT_MESSAGE) {
        getNode()->handleData(static_cast<const char*>(message.data()),
                              message.size());
      }
    }

    if (event.flags & uvw::PollHandle::Event::DISCONNECT) {
      log("Closing connection with publisher", spdlog::level::info);
      stop();
    }
  });

  synchronize_poller_->on<uvw::PollEvent>(
      [this](const uvw::PollEvent& event, uvw::PollHandle& handle) {
        if (subscriber_.synchronize_socket().getsockopt<int>(ZMQ_EVENTS) &
            ZMQ_POLLOUT) {
          ready_to_confirm_connection_ = true;
          if (subscriber_.isReady()) {
            confirmConnection();
          } else {
            synchronize_poller_->close();
          }
        }
      });
}

void SubscriberProducer::stop() {
  poller_->close();
  synchronize_poller_->close();
  getNode()->stop();
}

zmq::message_t SubscriberProducer::readMessage() {
  zmq::message_t message;
  try {
    message = TransportUtils::readMessage(subscriber_.subscriber_socket());
  } catch (const std::exception& e) { log(e.what(), spdlog::level::err); }

  log("Data received, size: " + std::to_string(message.size()),
      spdlog::level::info);
  return message;
}

void SubscriberProducer::confirmConnection() {
  try {
    TransportUtils::send(subscriber_.synchronize_socket(), "");
  } catch (const std::exception& e) { log(e.what(), spdlog::level::err); }

  synchronize_poller_->close();
}

void SubscriberProducer::fetchSocketEvents() {
  zmq::message_t message;
  while (subscriber_.subscriber_socket()
             .recv(message, zmq::recv_flags::dontwait)
             .has_value()) {}
}

}  // namespace stream_data_processor
