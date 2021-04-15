#pragma once

#include <memory>
#include <string>
#include <vector>

#include <uvw.hpp>

#include "unix_socket_client.h"

namespace stream_data_processor {

class UnixSocketServer {
 public:
  UnixSocketServer(std::shared_ptr<UnixSocketClientFactory> client_factory,
                   const std::string& socket_path, uvw::Loop* loop);

  void start();
  void stop();

 private:
  static const std::string SOCKET_LOCK_FILE_SUFFIX;
  static constexpr mode_t LOCK_FILE_MODE = 0644;

 private:
  std::shared_ptr<UnixSocketClientFactory> client_factory_;
  std::string socket_path_;
  std::shared_ptr<uvw::PipeHandle> socket_handle_;
  int socket_lock_fd_;
  std::vector<std::shared_ptr<UnixSocketClient>> clients_;
};

}  // namespace stream_data_processor
