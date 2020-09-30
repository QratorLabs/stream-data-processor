#pragma once

#include <memory>
#include <string>
#include <vector>

#include <uvw.hpp>

#include "unix_socket_client.h"

class UnixSocketServer {
 public:
  UnixSocketServer(std::shared_ptr<UnixSocketClientFactory> client_factory,
                   const std::string& socket_path,
                   uvw::Loop* loop);

  void start();
  void stop();

  ~UnixSocketServer();

 private:
  std::shared_ptr<UnixSocketClientFactory> client_factory_;
  int sockfd_;
  std::shared_ptr<uvw::PipeHandle> socket_handle_;
  std::vector<std::shared_ptr<UnixSocketClient>> clients_;
};
