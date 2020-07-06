#include <iostream>
#include <memory>

#include "server.h"

#include <arrow/api.h>

const std::chrono::duration<uint64_t> Server::SILENCE_TIMEOUT_(10);

Server::Server(const std::shared_ptr<uvw::Loop>& loop) : tcp_(loop->resource<uvw::TCPHandle>()) {
  tcp_->on<uvw::ListenEvent>([](const uvw::ListenEvent& event, uvw::TCPHandle& server) {
    std::cerr << "New connection!" << std::endl;

    auto client = server.loop().resource<uvw::TCPHandle>();
    auto buffer_builder = std::make_shared<arrow::BufferBuilder>();
    auto timer = client->loop().resource<uvw::TimerHandle>();
    timer->start(SILENCE_TIMEOUT_, SILENCE_TIMEOUT_);

    client->on<uvw::DataEvent>([buffer_builder, timer](const uvw::DataEvent& event, uvw::TCPHandle& client) {
      std::cerr << "Data received: " << event.data.get() << std::endl;
      buffer_builder->Append(event.data.get(), event.length);
      timer->again();
    });

    auto client_weak = std::weak_ptr<uvw::TCPHandle>(client);
    timer->on<uvw::TimerEvent>([buffer_builder, client_weak](const uvw::TimerEvent& event, uvw::TimerHandle& timer) {
      auto client = client_weak.lock();
      std::shared_ptr<arrow::Buffer> buffer;
      if (!buffer_builder->Finish(&buffer).ok()) {
        std::cerr << "Buffer constructing failed" << std::endl;
        return;
      } else if (buffer->size() == 0) {
        std::cerr << "No data to be sent" << std::endl;
        return;
      }

      std::cerr << "Sending data: " << buffer->data() << std::endl;
      client->write(reinterpret_cast<char *>(buffer->mutable_data()), buffer->size());
      timer.again();
    });

    client->once<uvw::ErrorEvent>([timer](const uvw::ErrorEvent& event, uvw::TCPHandle& client) {
      std::cerr << "Error: " << event.what() << std::endl;
      timer->stop();
      timer->close();
      client.close();
    });

    client->once<uvw::EndEvent>([timer](const uvw::EndEvent& event, uvw::TCPHandle& client) {
      std::cerr << "Connection closed!" << std::endl;
      timer->stop();
      timer->close();
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
