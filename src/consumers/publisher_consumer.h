#pragma once

#include <queue>

#include <arrow/api.h>

#include <uvw.hpp>

#include <zmq.hpp>

#include "consumer.h"
#include "utils/transport_utils.h"

namespace stream_data_processor {

using transport_utils::TransportUtils;

class PublisherConsumer : public Consumer {
 public:
  template <typename PublisherType>
  PublisherConsumer(PublisherType&& publisher, uvw::Loop* loop)
      : publisher_(std::forward<PublisherType>(publisher)),
        publisher_poller_(loop->resource<uvw::PollHandle>(
            publisher_.publisher_socket()->getsockopt<int>(ZMQ_FD))),
        connect_timer_(loop->resource<uvw::TimerHandle>()) {
    for (auto& synchronize_socket : publisher_.synchronize_sockets()) {
      synchronize_pollers_.push_back(loop->resource<uvw::PollHandle>(
          synchronize_socket->getsockopt<int>(ZMQ_FD)));
    }

    configureHandles();
  };

  void start() override;
  void consume(std::shared_ptr<arrow::Buffer> data) override;
  void stop() override;

 private:
  void configureHandles();

  void startSending();
  void flushBuffer();

 private:
  static const std::chrono::duration<uint64_t, std::milli> CONNECT_TIMEOUT;

  TransportUtils::Publisher publisher_;
  std::shared_ptr<uvw::PollHandle> publisher_poller_;
  std::shared_ptr<uvw::TimerHandle> connect_timer_;
  std::vector<std::shared_ptr<uvw::PollHandle>> synchronize_pollers_;
  std::queue<std::shared_ptr<arrow::Buffer>> data_buffers_;
  bool socket_is_writeable_{false};
};

}  // namespace stream_data_processor
