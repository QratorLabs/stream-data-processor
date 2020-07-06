#include <iostream>
#include <memory>

#include "server.h"

#include <arrow/api.h>

Server::Server(const std::shared_ptr<uvw::Loop>& loop) : tcp_(loop->resource<uvw::TCPHandle>()) {
  tcp_->on<uvw::ListenEvent>([](const uvw::ListenEvent& event, uvw::TCPHandle& server) {
    std::cerr << "New connection!" << std::endl;

    auto client = server.loop().resource<uvw::TCPHandle>();
    client->data(std::make_shared<arrow::BufferBuilder>());

    client->on<uvw::DataEvent>([](const uvw::DataEvent& event, uvw::TCPHandle& client) {
      std::cerr << "Data received: " << event.data.get() << std::endl;
      client.data<arrow::BufferBuilder>()->Append(event.data.get(), event.length);
      std::shared_ptr<arrow::Buffer> buffer;
      if (!client.data<arrow::BufferBuilder>()->Finish(&buffer).ok()) {
        std::cerr << "Buffer constructing failed" << std::endl;
        return;
      }
      client.write(reinterpret_cast<char *>(buffer->mutable_data()), buffer->size());
    });

    client->once<uvw::ErrorEvent>([](const uvw::ErrorEvent& event, uvw::TCPHandle& client) {
      std::cerr << "Error: " << event.what() << std::endl;
      client.close();
    });

    client->once<uvw::EndEvent>([](const uvw::EndEvent& event, uvw::TCPHandle& client) {
      std::cerr << "Connection closed!" << std::endl;
      client.close();
    });

    server.accept(*client);
    client->read();
  });
}

void Server::start(const std::string &host, size_t port) {
  tcp_->bind(host, port);
  tcp_->listen();
}
