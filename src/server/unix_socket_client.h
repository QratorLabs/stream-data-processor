#pragma once

#include <memory>

#include "uvw.hpp"

class UnixSocketClient {
 public:
  virtual void start() = 0;
  virtual void stop() = 0;
};

class UnixSocketClientFactory {
 public:
  virtual std::shared_ptr<UnixSocketClient> createClient(const std::shared_ptr<uvw::PipeHandle>& pipe_handle) = 0;
};
