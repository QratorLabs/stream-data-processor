#include <chrono>
#include <csignal>
#include <memory>

#include <spdlog/spdlog.h>

#include <uvw.hpp>

#include "kapacitor_udf/kapacitor_udf.h"
#include "server/unix_socket_client.h"
#include "server/unix_socket_server.h"

class BatchAggregateUDFAgentClientFactory : public UnixSocketClientFactory {
 public:
  std::shared_ptr<UnixSocketClient> createClient(
      const std::shared_ptr<uvw::PipeHandle>& pipe_handle) override {
    auto agent =
        std::make_shared<SocketBasedUDFAgent>(pipe_handle, pipe_handle);

    std::shared_ptr<RequestHandler> handler =
        std::make_shared<BatchAggregateRequestHandler>(agent);

    agent->setHandler(handler);
    return std::make_shared<AgentClient>(agent);
  }
};

class StreamAggregateUDFAgentClientFactory : public UnixSocketClientFactory {
 public:
  std::shared_ptr<UnixSocketClient> createClient(
      const std::shared_ptr<uvw::PipeHandle>& pipe_handle) override {
    auto agent =
        std::make_shared<SocketBasedUDFAgent>(pipe_handle, pipe_handle);

    std::shared_ptr<RequestHandler> handler =
        std::make_shared<StreamAggregateRequestHandler>(agent);

    agent->setHandler(handler);
    return std::make_shared<AgentClient>(agent);
  }
};

int main(int argc, char** argv) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::flush_every(std::chrono::seconds(5));

  if (argc < 3) {
    spdlog::error("Unix socket paths not provided");
    return 1;
  }

  auto loop = uvw::Loop::getDefault();
  std::string batch_socket_path(argv[1]);
  std::string stream_socket_path(argv[2]);

  UnixSocketServer batch_server(
      std::make_shared<BatchAggregateUDFAgentClientFactory>(),
      batch_socket_path,
      loop.get());

  UnixSocketServer stream_server(
      std::make_shared<StreamAggregateUDFAgentClientFactory>(),
      stream_socket_path,
      loop.get());

  auto signal_handle = loop->resource<uvw::SignalHandle>();
  signal_handle->on<uvw::SignalEvent>(
      [&](const uvw::SignalEvent& event, uvw::SignalHandle& handle) {
        if (event.signum == SIGINT) {
          spdlog::info("Caught SIGINT signal. Terminating...");
          batch_server.stop();
          stream_server.stop();
          signal_handle->stop();
          loop->stop();
        }
      });

  signal_handle->start(SIGINT);
  batch_server.start();
  stream_server.start();

  loop->run();

  return 0;
}
