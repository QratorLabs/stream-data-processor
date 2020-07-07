#include <iostream>

#include "pass_node.h"

const std::chrono::duration<uint64_t> PassNode::SILENCE_TIMEOUT(10);

PassNode::PassNode(const IPv4Endpoint &listen_endpoint, const std::vector<IPv4Endpoint> &target_endpoints)
    : loop_(uvw::Loop::getDefault())
    , server_(loop_->resource<uvw::TCPHandle>())
    , timer_(loop_->resource<uvw::TimerHandle>())
    , buffer_builder_(std::make_shared<arrow::BufferBuilder>()) {
  for (const auto& target_endpoint : target_endpoints) {
    addTarget(target_endpoint);
  }

  configureServer(listen_endpoint);
}

void PassNode::addTarget(const IPv4Endpoint &endpoint) {
  auto target = loop_->resource<uvw::TCPHandle>();

  target->once<uvw::ConnectEvent>([](const uvw::ConnectEvent& event, uvw::TCPHandle& target) {
    std::cerr << "Successfully connected to " << target.peer().port << std::endl;
  });

  target->on<uvw::ErrorEvent>([](const uvw::ErrorEvent& event, uvw::TCPHandle& target) {
    std::cerr << event.what() << std::endl;
  });

  target->connect(endpoint.host, endpoint.port);
  targets_.push_back(target);
}

void PassNode::configureServer(const IPv4Endpoint &endpoint) {
  timer_->on<uvw::TimerEvent>([this](const uvw::TimerEvent& event, uvw::TimerHandle& timer) {
    sendData();
    timer.again();
  });

  server_->once<uvw::ListenEvent>([this](const uvw::ListenEvent& event, uvw::TCPHandle& server) {
    std::cerr << "New connection!" << std::endl;
    auto client = server.loop().resource<uvw::TCPHandle>();

    client->on<uvw::DataEvent>([this](const uvw::DataEvent& event, uvw::TCPHandle& client) {
      std::cerr << "Data received: " << event.data.get() << std::endl;
      buffer_builder_->Append(event.data.get(), event.length);
      timer_->again();
    });

    client->once<uvw::ErrorEvent>([this](const uvw::ErrorEvent& event, uvw::TCPHandle& client) {
      std::cerr << event.what() << std::endl;
      sendData();
      stop();
      client.close();
    });

    client->once<uvw::EndEvent>([this](const uvw::EndEvent& event, uvw::TCPHandle& client) {
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

void PassNode::sendData() {
  std::shared_ptr<arrow::Buffer> buffer;
  if (!buffer_builder_->Finish(&buffer).ok() || buffer->size() == 0) {
    std::cerr << "No data to be sent" << std::endl;
    return;
  }

  for (auto& target : targets_) {
    std::cerr << "Sending data to " << target->peer().port << std::endl;
    target->write(reinterpret_cast<char *>(buffer->mutable_data()), buffer->size());
  }
}

void PassNode::stop() {
  timer_->stop();
  timer_->close();
  server_->close();
  for (auto& target : targets_) {
    target->close();
  }
}

void PassNode::start() {
  loop_->run();
}
