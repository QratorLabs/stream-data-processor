#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <utility>

#include <arrow/api.h>
#include <uvw.hpp>

class Server {
 public:
  explicit Server(const std::shared_ptr<uvw::Loop>& loop);

  void start(const std::string& host, size_t port);

 private:
  std::shared_ptr<uvw::TCPHandle> tcp_;

  static const std::chrono::duration<uint64_t> SILENCE_TIMEOUT;
};
