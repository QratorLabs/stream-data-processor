#include <utility>

#include <arrow/api.h>

#include <spdlog/spdlog.h>

#include "eval_node.h"
#include "utils/transport_utils.h"

const std::chrono::duration<uint64_t> EvalNode::SILENCE_TIMEOUT(10);

EvalNode::EvalNode(std::string name,
                   const std::shared_ptr<uvw::Loop>& loop,
                   TransportUtils::Subscriber&& subscriber,
                   TransportUtils::Publisher&& publisher,
                   std::shared_ptr<DataHandler> data_handler)
    : PassNodeBase(std::move(name), loop, std::move(subscriber), std::move(publisher))
    , data_handler_(std::move(data_handler))
    , timer_(loop->resource<uvw::TimerHandle>())
    , buffer_builder_(std::make_shared<arrow::BufferBuilder>()) {
  configureServer();
}

void EvalNode::configureServer() {
  timer_->on<uvw::TimerEvent>([this](const uvw::TimerEvent& event, uvw::TimerHandle& timer) {
    send();
  });

  poller_->on<uvw::PollEvent>([this](const uvw::PollEvent& event, uvw::PollHandle& poller) {
    if (subscriber_.subscriber_socket().getsockopt<int>(ZMQ_EVENTS) & ZMQ_POLLIN) {
      auto message = readMessage();
      if (!subscriber_.isReady()) {
        spdlog::get(name_)->info("Connected to publisher");
        subscriber_.confirmConnection();
        timer_->start(SILENCE_TIMEOUT, SILENCE_TIMEOUT);
      } else if (message.to_string() != TransportUtils::CONNECT_MESSAGE) {
        auto append_status = appendData(static_cast<const char *>(message.data()), message.size());
      }
    }

    if (event.flags & uvw::PollHandle::Event::DISCONNECT) {
      spdlog::get(name_)->info("Closed connection with publisher");
      send();
      stop();
    }
  });

  poller_->start(uvw::Flags<uvw::PollHandle::Event>::from<uvw::PollHandle::Event::READABLE, uvw::PollHandle::Event::DISCONNECT>());
}

void EvalNode::send() {
  std::shared_ptr<arrow::Buffer> processed_data;
  auto processing_status = processData(processed_data);
  if (!processing_status.ok()) {
    spdlog::get(name_)->error(processing_status.ToString());
    return;
  }

  sendData(processed_data);
}

void EvalNode::stop() {
  spdlog::get(name_)->info("Stopping node");
  timer_->stop();
  timer_->close();
  poller_->close();
  subscriber_.subscriber_socket().close();
  subscriber_.synchronize_socket().close();
  publisher_.publisher_socket().close();
  publisher_.synchronize_socket().close();
}

arrow::Status EvalNode::processData(std::shared_ptr<arrow::Buffer>& processed_data) {
  std::shared_ptr<arrow::Buffer> buffer;
  ARROW_RETURN_NOT_OK(buffer_builder_->Finish(&buffer));
  if (buffer->size() == 0) {
    return arrow::Status::CapacityError("No data to send");
  }

  ARROW_RETURN_NOT_OK(data_handler_->handle(buffer, &processed_data));

  return arrow::Status::OK();
}

arrow::Status EvalNode::appendData(const char *data, size_t length) {
  spdlog::get(name_)->debug("Appending data of size {} to buffer", length);
  ARROW_RETURN_NOT_OK(buffer_builder_->Append(data, length));
  return arrow::Status::OK();
}
