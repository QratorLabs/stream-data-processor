#pragma once

#include <memory>

#include <uvw.hpp>

namespace stream_data_processor {

class UnixSocketClient {
 public:
  virtual void start() = 0;
  virtual void stop() = 0;

  virtual ~UnixSocketClient() = 0;

 protected:
  UnixSocketClient() = default;

  UnixSocketClient(const UnixSocketClient& /* non-used */) = default;
  UnixSocketClient& operator=(const UnixSocketClient& /* non-used */) =
      default;

  UnixSocketClient(UnixSocketClient&& /* non-used */) = default;
  UnixSocketClient& operator=(UnixSocketClient&& /* non-used */) = default;
};

class UnixSocketClientFactory {
 public:
  virtual std::shared_ptr<UnixSocketClient> createClient(
      const std::shared_ptr<uvw::PipeHandle>& pipe_handle) = 0;
  virtual ~UnixSocketClientFactory() = 0;

 protected:
  UnixSocketClientFactory() = default;

  UnixSocketClientFactory(const UnixSocketClientFactory& /* non-used */) =
      default;
  UnixSocketClientFactory& operator=(
      const UnixSocketClientFactory& /* non-used */) = default;

  UnixSocketClientFactory(UnixSocketClientFactory&& /* non-used */) = default;
  UnixSocketClientFactory& operator=(
      UnixSocketClientFactory&& /* non-used */) = default;
};

}  // namespace stream_data_processor
