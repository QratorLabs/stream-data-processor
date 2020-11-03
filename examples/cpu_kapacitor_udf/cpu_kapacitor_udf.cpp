#include <chrono>
#include <csignal>
#include <iostream>
#include <memory>

#include <spdlog/spdlog.h>
#include <cxxopts.hpp>
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
  cxxopts::Options options("AggregateUDF", "Aggregates data from kapacitor");
  options.add_options()
  ("b,batch", "Unix socket path for batch data",
      cxxopts::value<std::string>())
  ("s,stream", "Unix socket path for stream data",
      cxxopts::value<std::string>())
  ("v,verbose", "Enable detailed logging")
  ("h,help", "Print this message")
  ;

  std::string batch_socket_path, stream_socket_path;
  try {
    auto arguments_parse_result = options.parse(argc, argv);
    if (arguments_parse_result["help"].as<bool>()) {
      std::cout << options.help() << std::endl;
      return 0;
    }

    if (arguments_parse_result["verbose"].as<bool>()) {
      spdlog::set_level(spdlog::level::debug);
    }

    batch_socket_path = arguments_parse_result["batch"].as<std::string>();
    stream_socket_path = arguments_parse_result["stream"].as<std::string>();
  } catch (const cxxopts::OptionException& exc) {
    std::cerr << exc.what() << std::endl;
    std::cout << options.help() << std::endl;
    return 1;
  }

  spdlog::flush_every(std::chrono::seconds(5));

  auto loop = uvw::Loop::getDefault();

  UnixSocketServer batch_server(
      std::make_shared<BatchAggregateUDFAgentClientFactory>(),
      batch_socket_path, loop.get());

  UnixSocketServer stream_server(
      std::make_shared<StreamAggregateUDFAgentClientFactory>(),
      stream_socket_path, loop.get());

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
