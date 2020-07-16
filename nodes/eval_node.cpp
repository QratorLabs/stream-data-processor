#include <utility>

#include <arrow/api.h>

#include <spdlog/spdlog.h>

#include "eval_node.h"

const std::chrono::duration<uint64_t> EvalNode::SILENCE_TIMEOUT(10);

EvalNode::EvalNode(std::string name,
    std::shared_ptr<uvw::Loop> loop,
    std::shared_ptr<DataHandler> data_handler,
    const IPv4Endpoint &listen_endpoint,
    const std::vector<IPv4Endpoint> &target_endpoints)
    : name_(std::move(name))
    , logger_(spdlog::basic_logger_mt(name_, "logs/" + name_ + ".txt", true))
    , loop_(std::move(loop))
    , data_handler_(std::move(data_handler))
    , server_(loop_->resource<uvw::TCPHandle>())
    , timer_(loop_->resource<uvw::TimerHandle>())
    , buffer_builder_(std::make_shared<arrow::BufferBuilder>()) {
  for (const auto& target_endpoint : target_endpoints) {
    addTarget(target_endpoint);
  }

  configureServer(listen_endpoint);
}

void EvalNode::addTarget(const IPv4Endpoint &endpoint) {
  auto target = loop_->resource<uvw::TCPHandle>();

  target->once<uvw::ConnectEvent>([this, &endpoint](const uvw::ConnectEvent& event, uvw::TCPHandle& target) {
    spdlog::get(name_)->info("Successfully connected to {}:{}", endpoint.host, endpoint.port);
  });

  target->on<uvw::ErrorEvent>([this](const uvw::ErrorEvent& event, uvw::TCPHandle& target) {
    spdlog::get(name_)->error(event.what());
  });

  target->connect(endpoint.host, endpoint.port);
  targets_.push_back(target);
}

void EvalNode::configureServer(const IPv4Endpoint &endpoint) {
  timer_->on<uvw::TimerEvent>([this](const uvw::TimerEvent& event, uvw::TimerHandle& timer) {
    sendData();
  });

  server_->once<uvw::ListenEvent>([this](const uvw::ListenEvent& event, uvw::TCPHandle& server) {
    spdlog::get(name_)->info("New client connection");

    auto client = server.loop().resource<uvw::TCPHandle>();

    client->on<uvw::DataEvent>([this](const uvw::DataEvent& event, uvw::TCPHandle& client) {
      spdlog::get(name_)->debug("Data received, size: {}", event.length);
      buffer_builder_->Append(event.data.get(), event.length);
    });

    client->once<uvw::ErrorEvent>([this](const uvw::ErrorEvent& event, uvw::TCPHandle& client) {
      spdlog::get(name_)->error("Error: {}", event.what());
      sendData();
      stop();
      client.close();
    });

    client->once<uvw::EndEvent>([this](const uvw::EndEvent& event, uvw::TCPHandle& client) {
      spdlog::get(name_)->info("Closed connection with client");
      sendData();
      stop();
      client.close();
    });

    timer_->start(SILENCE_TIMEOUT, SILENCE_TIMEOUT);
    server.accept(*client);
    client->read();
  });

  server_->bind(endpoint.host, endpoint.port);
  server_->listen();
}

void EvalNode::sendData() {
  std::shared_ptr<arrow::Buffer> buffer;
  if (!buffer_builder_->Finish(&buffer).ok() || buffer->size() == 0) {
    spdlog::get(name_)->debug("No data to send");
    return;
  }

  std::shared_ptr<arrow::Buffer> processed_data;
  auto handle_result = data_handler_->handle(buffer, &processed_data);
  if (!handle_result.ok()) {
    spdlog::get(name_)->debug(handle_result.ToString());
    return;
  }

  spdlog::get(name_)->info("Sending data");
  for (auto &target : targets_) {
    target->write(reinterpret_cast<char *>(processed_data->mutable_data()), processed_data->size());
  }
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
