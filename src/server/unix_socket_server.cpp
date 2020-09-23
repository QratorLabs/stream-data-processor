#include <sys/socket.h>

#include "unix_socket_server.h"

UnixSocketServer::UnixSocketServer(std::shared_ptr<UnixSocketClientFactory> client_factory,
                                   const std::string& socket_path,
                                   const std::shared_ptr<uvw::Loop>& loop)
    : client_factory_(std::move(client_factory))
    , sockfd_(socket(PF_LOCAL, SOCK_STREAM, 0))
    , socket_handle_(loop->resource<uvw::PipeHandle>()) {
  socket_handle_->on<uvw::ListenEvent>([this](const uvw::ListenEvent& event, uvw::PipeHandle& socket_handle) {
    auto connection = socket_handle.loop().resource<uvw::PipeHandle>();
    clients_.push_back(std::move(client_factory_->createClient(connection)));
    socket_handle_->accept(*connection);
    clients_.back()->start();
  });

  socket_handle_->bind(socket_path);
}

void UnixSocketServer::start() {
  socket_handle_->listen();
}

void UnixSocketServer::stop() {
  for (auto& client : clients_) {
    client->stop();
  }
}
