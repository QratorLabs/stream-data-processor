#include <utility>

#include <arrow/api.h>

#include <spdlog/spdlog.h>

#include "eval_node.h"
#include "utils/network_utils.h"

const std::chrono::duration<uint64_t> EvalNode::SILENCE_TIMEOUT(10);

EvalNode::EvalNode(std::string name,
    const std::shared_ptr<uvw::Loop>& loop,
    const IPv4Endpoint &listen_endpoint,
    const std::vector<IPv4Endpoint> &target_endpoints,
    std::shared_ptr<DataHandler> data_handler,
    bool is_input_node)
    : PassNodeBase(std::move(name), loop, listen_endpoint, target_endpoints)
    , data_handler_(std::move(data_handler))
    , timer_(loop->resource<uvw::TimerHandle>())
    , buffer_builder_(std::make_shared<arrow::BufferBuilder>())
    , is_input_node_(is_input_node) {
  configureServer();
}

void EvalNode::configureServer() {
  timer_->on<uvw::TimerEvent>([this](const uvw::TimerEvent& event, uvw::TimerHandle& timer) {
    send();
  });

  server_->once<uvw::ListenEvent>([this](const uvw::ListenEvent& event, uvw::TCPHandle& server) {
    spdlog::get(name_)->info("New client connection");

    auto client = server.loop().resource<uvw::TCPHandle>();

    client->on<uvw::DataEvent>([this](const uvw::DataEvent& event, uvw::TCPHandle& client) {
      spdlog::get(name_)->info("Data received, size: {}", event.length);
      auto append_status = appendData(event.data.get(), event.length);
      if (!append_status.ok()) {
        spdlog::get(name_)->error(append_status.ToString());
      }
    });

    client->once<uvw::ErrorEvent>([this](const uvw::ErrorEvent& event, uvw::TCPHandle& client) {
      spdlog::get(name_)->error(event.what());
      send();
      stop();
      client.close();
    });

    client->once<uvw::EndEvent>([this](const uvw::EndEvent& event, uvw::TCPHandle& client) {
      spdlog::get(name_)->info("Closed connection with client");
      send();
      stop();
      client.close();
    });

    timer_->start(SILENCE_TIMEOUT, SILENCE_TIMEOUT);
    server.accept(*client);
    client->read();
  });
}

void EvalNode::send() {
  std::shared_ptr<arrow::Buffer> processed_data;
  auto processing_status = processData(processed_data);
  if (!processing_status.ok()) {
    spdlog::get(name_)->debug(processing_status.ToString());
    return;
  }

  sendData(processed_data);
}

void EvalNode::stop() {
  spdlog::get(name_)->info("Stopping node");
  timer_->stop();
  timer_->close();
  server_->close();
  for (auto& target : targets_) {
    target->close();
  }
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
  if (is_input_node_) {
    ARROW_RETURN_NOT_OK(buffer_builder_->Append(data, length));
    return arrow::Status::OK();
  }

  for (auto& data_part : NetworkUtils::splitMessage(data, length)) {
    spdlog::get(name_)->debug("Appending data of size {} to buffer", data_part.second);
    auto append_status = buffer_builder_->Append(data_part.first, data_part.second);
    if (!append_status.ok()) {
      spdlog::get(name_)->error(append_status.ToString());
    }
  }

  return arrow::Status::OK();
}
