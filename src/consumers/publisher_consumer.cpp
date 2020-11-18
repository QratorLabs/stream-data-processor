#include "publisher_consumer.h"

namespace stream_data_processor {

using transport_utils::TransportUtils;

const std::chrono::duration<uint64_t, std::milli>
    PublisherConsumer::CONNECT_TIMEOUT{1000};

void PublisherConsumer::start() {
  publisher_poller_->start(uvw::Flags<uvw::PollHandle::Event>::from<
                           uvw::PollHandle::Event::WRITABLE>());
  for (auto& poller : synchronize_pollers_) {
    poller->start(uvw::Flags<uvw::PollHandle::Event>::from<
                  uvw::PollHandle::Event::READABLE,
                  uvw::PollHandle::Event::DISCONNECT>());
  }

  connect_timer_->start(CONNECT_TIMEOUT, CONNECT_TIMEOUT);
}

void PublisherConsumer::consume(std::shared_ptr<arrow::Buffer> data) {
  data_buffers_.push(std::move(data));
}

void PublisherConsumer::stop() {
  startSending();

  if (!TransportUtils::send(*publisher_.publisher_socket(),
                            TransportUtils::END_MESSAGE)) {
    throw std::runtime_error("Error while sending, error code: " +
                             std::to_string(zmq_errno()));
  }

  connect_timer_->close();
  publisher_poller_->close();
  for (size_t i = 0; i < synchronize_pollers_.size(); ++i) {
    synchronize_pollers_[i]->close();
  }
}

void PublisherConsumer::configureHandles() {
  connect_timer_->on<uvw::TimerEvent>(
      [this](const uvw::TimerEvent& event, uvw::TimerHandle& timer) {
        if (!publisher_.isReady() && socket_is_writeable_) {
          publisher_.trySynchronize();
          socket_is_writeable_ = false;
        } else if (publisher_.isReady()) {
          connect_timer_->stop();
        }
      });

  publisher_poller_->on<uvw::PollEvent>(
      [this](const uvw::PollEvent& event, uvw::PollHandle& poller) {
        startSending();
      });

  for (size_t i = 0; i < synchronize_pollers_.size(); ++i) {
    synchronize_pollers_[i]->on<uvw::PollEvent>(
        [this, i](const uvw::PollEvent& event, uvw::PollHandle& poller) {
          if (publisher_.synchronize_sockets()[i]->getsockopt<int>(
                  ZMQ_EVENTS) &
              ZMQ_POLLIN) {
            TransportUtils::readMessage(*publisher_.synchronize_sockets()[i]);
            publisher_.addConnection();
            synchronize_pollers_[i]->close();
            return;
          }

          if (event.flags & uvw::PollHandle::Event::DISCONNECT) {
            synchronize_pollers_[i]->close();
          }
        });
  }
}

void PublisherConsumer::startSending() {
  while (publisher_.publisher_socket()->getsockopt<int>(ZMQ_EVENTS) &
         ZMQ_POLLOUT) {
    socket_is_writeable_ = true;
    if (publisher_.isReady() && !data_buffers_.empty()) {
      flushBuffer();
    } else {
      break;
    }
  }
}

void PublisherConsumer::flushBuffer() {
  if (!socket_is_writeable_ || data_buffers_.empty()) {
    return;
  }

  if (TransportUtils::send(*publisher_.publisher_socket(),
                           data_buffers_.front()->ToString())) {
    socket_is_writeable_ = false;
    data_buffers_.pop();
  } else {
    throw std::runtime_error("Error while sending, error code: " +
                             std::to_string(zmq_errno()));
  }
}

}  // namespace stream_data_processor
