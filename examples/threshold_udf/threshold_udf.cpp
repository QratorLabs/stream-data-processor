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

namespace sdp = stream_data_processor;
namespace udf = sdp::kapacitor_udf;

class ThresholdUDFAgentClientFactory : public sdp::UnixSocketClientFactory {
 public:
  std::shared_ptr<sdp::UnixSocketClient> createClient(
      const std::shared_ptr<uvw::PipeHandle>& pipe_handle) override {
    auto agent =
        std::make_shared<udf::SocketBasedUDFAgent>(pipe_handle, pipe_handle);

    std::shared_ptr<udf::RequestHandler> handler =
        std::make_shared<udf::StatefulThresholdRequestHandler>(agent);

    agent->setHandler(handler);
    return std::make_shared<udf::AgentClient>(agent);
  }
};

int main(int argc, char** argv) {
  cxxopts::Options options("ThresholdUDF", "Adjusts threshold for current state and adds it to the stream");
  options.add_options()
  ("s,socket", "Unix socket path for communication",
      cxxopts::value<std::string>())
  ("v,verbose", "Enable detailed logging")
  ("h,help", "Print this message")
  ;

  std::string socket_path;
  try {
    auto arguments_parse_result = options.parse(argc, argv);
    if (arguments_parse_result["help"].as<bool>()) {
      std::cout << options.help() << std::endl;
      return 0;
    }

    if (arguments_parse_result["verbose"].as<bool>()) {
      spdlog::set_level(spdlog::level::debug);
    }

    socket_path = arguments_parse_result["socket"].as<std::string>();
  } catch (const cxxopts::OptionException& exc) {
    std::cerr << exc.what() << std::endl;
    std::cout << options.help() << std::endl;
    return 1;
  }

  spdlog::flush_every(std::chrono::seconds(5));

  auto loop = uvw::Loop::getDefault();

  sdp::UnixSocketServer server(
      std::make_shared<ThresholdUDFAgentClientFactory>(),
      socket_path, loop.get());

  auto signal_handle = loop->resource<uvw::SignalHandle>();
  signal_handle->on<uvw::SignalEvent>(
      [&](const uvw::SignalEvent& event, uvw::SignalHandle& handle) {
        if (event.signum == SIGINT) {
          spdlog::info("Caught SIGINT signal. Terminating...");
          server.stop();
          signal_handle->stop();
          loop->stop();
        }
      });

  signal_handle->start(SIGINT);
  server.start();

  loop->run();

  return 0;
}
