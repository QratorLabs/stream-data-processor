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
  std::shared_ptr<UnixSocketClientFactory> client_factory_;
  std::shared_ptr<uvw::PipeHandle> socket_handle_;
  std::vector<std::shared_ptr<UnixSocketClient>> clients_;
};

}  // namespace stream_data_processor