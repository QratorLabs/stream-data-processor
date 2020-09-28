#pragma once

#include <queue>

#include <arrow/api.h>

#include <uvw.hpp>

#include <zmq.hpp>

#include "consumer.h"
#include "utils/transport_utils.h"

class PublisherConsumer : public Consumer {
 public:
  template <typename U>
  PublisherConsumer(U&& publisher, const std::shared_ptr<uvw::Loop>& loop)
      : publisher_(std::forward<U>(publisher))
      , publisher_poller_(loop->resource<uvw::PollHandle>(publisher_.publisher_socket()->getsockopt<int>(ZMQ_FD)))
      , connect_timer_(loop->resource<uvw::TimerHandle>()) {
    for (auto& synchronize_socket : publisher_.synchronize_sockets()) {
      synchronize_pollers_.push_back(
          loop->resource<uvw::PollHandle>(synchronize_socket->getsockopt<int>(ZMQ_FD))
      );
    }

    configureHandles();
  };

  void start() override;
  void consume(const char* data, size_t length) override;
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


