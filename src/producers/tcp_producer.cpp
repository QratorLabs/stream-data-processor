#include "tcp_producer.h"
#include "utils/transport_utils.h"

TCPProducer::TCPProducer(const std::shared_ptr<Node>& node, const IPv4Endpoint &listen_endpoint,
                         uvw::Loop* loop, bool is_external)
    : Producer(node)
    , listener_(loop->resource<uvw::TCPHandle>())
    , is_external_(is_external) {
  configureListener();
  listener_->bind(listen_endpoint.host, listen_endpoint.port);
}

void TCPProducer::start() {
  listener_->listen();
}

void TCPProducer::stop() {
  listener_->close();
  node_->stop();
}

void TCPProducer::configureListener() {
  listener_->once<uvw::ListenEvent>([this](const uvw::ListenEvent& event, uvw::TCPHandle& server) {
    node_->log("New client connection", spdlog::level::info);

    auto client = server.loop().resource<uvw::TCPHandle>();

    client->on<uvw::DataEvent>([this](const uvw::DataEvent& event, uvw::TCPHandle& client) {
      node_->log("Data received, size: " + std::to_string(event.length), spdlog::level::info);
      handleData(event.data.get(), event.length);
    });

    client->once<uvw::ErrorEvent>([this](const uvw::ErrorEvent& event, uvw::TCPHandle& client) {
      node_->log("Error code: " + std::to_string(event.code()) + ". " + event.what(), spdlog::level::err);
      stop();
      client.close();
    });

    client->once<uvw::EndEvent>([this](const uvw::EndEvent& event, uvw::TCPHandle& client) {
      node_->log("Closing connection with client", spdlog::level::info);
      stop();
      client.close();
    });

    server.accept(*client);
    client->read();
  });
}

void TCPProducer::handleData(const char *data, size_t length) {
  if (is_external_) {
    node_->handleData(data, length);
  } else {
    for (auto& [data, length] : TransportUtils::splitMessage(data, length)) {
      node_->handleData(data, length);
    }
  }
}
