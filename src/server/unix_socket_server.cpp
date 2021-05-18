#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <spdlog/spdlog.h>
#include <uvw/fs.h>

#include "unix_socket_server.h"

namespace stream_data_processor {

const std::string UnixSocketServer::SOCKET_LOCK_FILE_SUFFIX = ".lock";

UnixSocketServer::UnixSocketServer(
    std::shared_ptr<UnixSocketClientFactory> client_factory,
    const std::string& socket_path, uvw::Loop* loop)
    : client_factory_(std::move(client_factory)),
      socket_path_(socket_path),
      socket_handle_(loop->resource<uvw::PipeHandle>()),
      listen_event_listener_(socket_handle_->on<uvw::ListenEvent>(
          [this](const uvw::ListenEvent& event,
                 uvw::PipeHandle& socket_handle) {
            handleNewConnection(event, socket_handle);
          })) {
  socket_handle_->on<uvw::ErrorEvent>(
      [](const uvw::ErrorEvent& event, uvw::PipeHandle& socket_handle) {
        spdlog::error(event.what());
      });
}

UnixSocketServer::UnixSocketServer(UnixSocketServer&& other)
    : client_factory_(std::move(other.client_factory_)),
      socket_path_(std::move(other.socket_path_)),
      socket_handle_(std::move(other.socket_handle_)),
      socket_lock_fd_(other.socket_lock_fd_),
      clients_(std::move(other.clients_)) {
  socket_handle_->erase(other.listen_event_listener_);
  listen_event_listener_ = socket_handle_->on<uvw::ListenEvent>(
      [this](const uvw::ListenEvent& event, uvw::PipeHandle& socket_handle) {
        handleNewConnection(event, socket_handle);
      });
}

UnixSocketServer& UnixSocketServer::operator=(UnixSocketServer&& other) {
  if (&other == this) {
    return *this;
  }

  client_factory_ = std::move(other.client_factory_);
  socket_path_ = std::move(other.socket_path_);
  socket_handle_ = std::move(other.socket_handle_);
  socket_lock_fd_ = other.socket_lock_fd_;
  clients_ = std::move(other.clients_);
  socket_handle_->erase(other.listen_event_listener_);

  listen_event_listener_ = socket_handle_->on<uvw::ListenEvent>(
      [this](const uvw::ListenEvent& event, uvw::PipeHandle& socket_handle) {
        handleNewConnection(event, socket_handle);
      });

  return *this;
}

void UnixSocketServer::start() {
  auto lock_path = socket_path_ + SOCKET_LOCK_FILE_SUFFIX;
  socket_lock_fd_ =
      open(lock_path.c_str(), O_CREAT | O_RDONLY, LOCK_FILE_MODE);

  if (socket_lock_fd_ == -1) {
    spdlog::error(
        "Failed to start server: can't open lock file: {}, errno: {}",
        lock_path, errno);
    return;
  }

  auto lock_result = flock(socket_lock_fd_, LOCK_EX | LOCK_NB);
  if (lock_result == -1) {
    spdlog::error(
        "Failed to start server: can't obtain lock on file: {}, "
        "errno: {}. Probably, there are more than one instance of server "
        "using socket by path {}",
        lock_path, errno, socket_path_);
    close(socket_lock_fd_);
    return;
  }

  if (!socket_handle_->loop().resource<uvw::FsReq>()->unlinkSync(
          socket_path_) &&
      errno != ENOENT) {
    spdlog::error(
        "Failed to start server: can't unlink UNIX socket: {}, "
        "errno: {}",
        socket_path_, errno);
    close(socket_lock_fd_);
    return;
  }

  socket_handle_->bind(socket_path_);
  socket_handle_->listen();
  spdlog::info("Server is started");
}

void UnixSocketServer::stop() {
  for (auto& client : clients_) { client->stop(); }
  socket_handle_->close();
  flock(socket_lock_fd_, LOCK_UN);
  close(socket_lock_fd_);
  spdlog::info("Server is stopped");
}

}  // namespace stream_data_processor
