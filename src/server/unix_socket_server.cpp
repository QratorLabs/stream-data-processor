#include <spdlog/spdlog.h>
#include <uvw/fs.h>

#include "unix_socket_server.h"

namespace stream_data_processor {

UnixSocketServer::UnixSocketServer(
    std::shared_ptr<UnixSocketClientFactory> client_factory,
    const std::string& socket_path, uvw::Loop* loop)
    : client_factory_(std::move(client_factory)),
      socket_handle_(loop->resource<uvw::PipeHandle>()) {
  socket_handle_->on<uvw::ListenEvent>(
      [this](const uvw::ListenEvent& event, uvw::PipeHandle& socket_handle) {
        spdlog::info("New socket connection!");
        auto connection = socket_handle.loop().resource<uvw::PipeHandle>();
        clients_.push_back(
            std::move(client_factory_->createClient(connection)));
        socket_handle_->accept(*connection);
        clients_.back()->start();
      });

  socket_handle_->on<uvw::ErrorEvent>(
      [](const uvw::ErrorEvent& event, uvw::PipeHandle& socket_handle) {
        spdlog::error(event.what());
      });

  loop->resource<uvw::FsReq>()->unlinkSync(socket_path);
  socket_handle_->bind(socket_path);
}

void UnixSocketServer::start() {
  socket_handle_->listen();
  spdlog::info("Server is started");
}

void UnixSocketServer::stop() {
  socket_handle_->close();
  for (auto& client : clients_) { client->stop(); }
  spdlog::info("Server is stopped");
}

}  // namespace stream_data_processor
