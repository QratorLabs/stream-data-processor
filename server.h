#pragma once

#include <memory>
#include <string>

#include <uvw.hpp>

class Server {
 public:
  explicit Server(const std::shared_ptr<uvw::Loop>& loop);

  void start(const std::string& host, size_t port);

 private:
  std::shared_ptr<uvw::TCPHandle> tcp_;
};


