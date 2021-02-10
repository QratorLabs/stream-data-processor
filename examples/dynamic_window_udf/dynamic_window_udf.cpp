#include <chrono>
#include <csignal>
#include <exception>
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

class DynamicWindowUDFAgentClientFactory : public sdp::UnixSocketClientFactory {
 public:
  explicit DynamicWindowUDFAgentClientFactory(uvw::Loop* loop)
      : loop_(loop) {
  }

  std::shared_ptr<sdp::UnixSocketClient> createClient(
      const std::shared_ptr<uvw::PipeHandle>& pipe_handle) override {
    auto agent =
        std::make_shared<udf::SocketBasedUDFAgent>(pipe_handle, pipe_handle);

    std::shared_ptr<udf::RequestHandler> handler =
        std::make_shared<udf::DynamicWindowRequestHandler>(agent.get(), loop_);

    agent->setHandler(handler);
    return std::make_shared<udf::AgentClient>(agent);
  }

 private:
  uvw::Loop* loop_;
};

int main(int argc, char** argv) {
  cxxopts::Options options("DynamicWindowUDF", "Emits window according to dynamic window options");
  options.add_options()
      ("s,socket", "Unix socket path for kapacitor communication",
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
  } catch (const std::exception& exc) {
    std::cerr << exc.what() << std::endl;
    std::cout << options.help() << std::endl;
    return 1;
  }

  spdlog::flush_every(std::chrono::seconds(5));

  auto loop = uvw::Loop::getDefault();

  sdp::UnixSocketServer server(
      std::make_shared<DynamicWindowUDFAgentClientFactory>(loop.get()),
      socket_path, loop.get());

  auto signal_handle = loop->resource<uvw::SignalHandle>();
  signal_handle->on<uvw::SignalEvent>(
      [&](const uvw::SignalEvent& event, uvw::SignalHandle& handle) {
        if (event.signum == SIGINT || event.signum == SIGTERM) {
          spdlog::info("Caught stop signal. Terminating...");
          server.stop();
          signal_handle->stop();
          loop->stop();
        }
      });

  signal_handle->start(SIGINT);
  signal_handle->start(SIGTERM);
  server.start();

  try {
    loop->run();
  } catch (const std::exception& exc) {
    server.stop();
    std::cerr << exc.what() << std::endl;
    return 1;
  }

  return 0;
}
