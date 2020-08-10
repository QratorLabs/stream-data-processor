#include "publisher_consumer.h"

const std::chrono::duration<uint64_t, std::milli> PublisherConsumer::CONNECT_TIMEOUT{1000};

void PublisherConsumer::start() {
  connect_timer_->start(CONNECT_TIMEOUT, CONNECT_TIMEOUT);
  for (auto& poller : synchronize_pollers_) {
    poller->start(uvw::Flags<uvw::PollHandle::Event>::from<
        uvw::PollHandle::Event::READABLE, uvw::PollHandle::Event::DISCONNECT
        >());
  }
}

void PublisherConsumer::consume(const char *data, size_t length) {
  if (!publisher_.isReady()) {
    data_buffers_.push_back(std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(data), length));
    return;
  }

  flushBuffers();

  zmq::message_t message(length);
  memcpy(message.data(), data, length);
  auto send_result = publisher_.publisher_socket()->send(message, zmq::send_flags::none);
  if (!send_result.has_value()) {
    throw std::runtime_error("Error while sending, error code: " + std::to_string(zmq_errno()));
  }
}

void PublisherConsumer::stop() {
  flushBuffers();

  zmq::message_t message(TransportUtils::END_MESSAGE.size());
  memcpy(message.data(), TransportUtils::END_MESSAGE.data(), TransportUtils::END_MESSAGE.size());
  auto send_result = publisher_.publisher_socket()->send(message, zmq::send_flags::none);
  if (!send_result.has_value()) {
    throw std::runtime_error("Error while sending, error code: " + std::to_string(zmq_errno()));
  }

  publisher_.publisher_socket()->close();
  for (size_t i = 0; i < synchronize_pollers_.size(); ++i) {
    synchronize_pollers_[i]->close();
    publisher_.synchronize_sockets()[i]->close();
  }

  connect_timer_->close();
}

void PublisherConsumer::configureHandles() {
  connect_timer_->on<uvw::TimerEvent>([this](const uvw::TimerEvent& event, uvw::TimerHandle& timer) {
    if (publisher_.trySynchronize()) {
      connect_timer_->stop();
    }
  });

  for (size_t i = 0; i < synchronize_pollers_.size(); ++i) {
    synchronize_pollers_[i]->on<uvw::PollEvent>([this, &i](const uvw::PollEvent &event, uvw::PollHandle &poller) {
      if (publisher_.synchronize_sockets()[i]->getsockopt<int>(ZMQ_EVENTS) & ZMQ_POLLIN) {
        TransportUtils::readMessage(*publisher_.synchronize_sockets()[i]);
        publisher_.addConnection();
        synchronize_pollers_[i]->close();
        publisher_.synchronize_sockets()[i]->close();
        return;
      }

      if (event.flags & uvw::PollHandle::Event::DISCONNECT) {
        synchronize_pollers_[i]->close();
        publisher_.synchronize_sockets()[i]->close();
      }
    });
  }
}

void PublisherConsumer::flushBuffers() {
  for (auto& buffer : data_buffers_) {
    if (!TransportUtils::send(*publisher_.publisher_socket(), buffer->ToString())) {
      throw std::runtime_error("Error while sending, error code: " + std::to_string(zmq_errno()));
    }
  }

  data_buffers_.clear();
}
