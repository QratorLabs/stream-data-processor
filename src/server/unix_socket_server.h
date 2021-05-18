#pragma once

#include <memory>
#include <string>
#include <vector>

#include <uvw.hpp>

#include "unix_socket_client.h"

namespace stream_data_processor {

class UnixSocketServer {
 public:
  UnixSocketServer() = delete;

  UnixSocketServer(std::shared_ptr<UnixSocketClientFactory> client_factory,
                   const std::string& socket_path, uvw::Loop* loop);

  UnixSocketServer(const UnixSocketServer& other) = delete;
  UnixSocketServer& operator=(const UnixSocketServer& other) = delete;

  UnixSocketServer(UnixSocketServer&& other);
  UnixSocketServer& operator=(UnixSocketServer&& other);

  ~UnixSocketServer() = default;

  void start();
  void stop();

 private:
  void handleNewConnection(const uvw::ListenEvent& event,
                           uvw::PipeHandle& socket_handle) {
    spdlog::info("New socket connection!");
    auto connection = socket_handle.loop().resource<uvw::PipeHandle>();
    clients_.push_back(client_factory_->createClient(connection));
    socket_handle_->accept(*connection);
    clients_.back()->start();
  }

 private:
  static const std::string SOCKET_LOCK_FILE_SUFFIX;
  static constexpr mode_t LOCK_FILE_MODE = 0644;

 private:
  std::shared_ptr<UnixSocketClientFactory> client_factory_;
  std::string socket_path_;
  std::shared_ptr<uvw::PipeHandle> socket_handle_;
  uvw::PipeHandle::Connection<uvw::ListenEvent> listen_event_listener_;
  int socket_lock_fd_;
  std::vector<std::shared_ptr<UnixSocketClient>> clients_;
};

}  // namespace stream_data_processor
