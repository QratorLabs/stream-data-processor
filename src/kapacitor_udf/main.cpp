#include <csignal>
#include <exception>
#include <iostream>
#include <memory>
#include <unordered_map>
#include <vector>

#include <spdlog/spdlog.h>
#include <cxxopts.hpp>
#include <uvw.hpp>

#include "kapacitor_udf/udf_agent_client_factory.h"
#include "server/unix_socket_client.h"
#include "server/unix_socket_server.h"

namespace sdp = stream_data_processor;
namespace udf = sdp::kapacitor_udf;

struct UDFProperties {
  std::string description;
  std::shared_ptr<sdp::UnixSocketClientFactory> client_factory;
};

int main(int argc, char** argv) {
  auto loop = uvw::Loop::getDefault();

  const std::unordered_map<std::string, UDFProperties>
      options_to_udf_properties{
          {"batch-aggr",
           {"[batchAggregateUDF] Unix socket path for aggregating batch data",
            std::make_shared<udf::BatchAggregateUDFAgentClientFactory>()}},
          {"stream-aggr",
           {"[streamAggregateUDF] Unix socket path for aggregating stream "
            "data",
            std::make_shared<udf::StreamAggregateUDFAgentClientFactory>(
                loop.get())}},
          {"window",
           {"[dynamicWindowUDF] Unix socket path for emitting windows with "
            "properties based on incoming data",
            std::make_shared<udf::DynamicWindowUDFAgentClientFactory>(
                loop.get())}},
          {"adj-level",
           {"[adjustLevelUDF] Unix socket path for adjusting alert level "
            "based on current field value",
            std::make_shared<udf::ThresholdUDFAgentClientFactory>()}},
          {"der",
           {"[DerivativeUDF] Unix socket path for calculating derivatives",
            std::make_shared<udf::DerivativeUDFAgentClientFactory>(
                loop.get())}}};

  cxxopts::Options options(
      "sdp-udf",
      "stream-data-processor runtime for executing a set of Kapactior UDFs");

  for (auto& [option_name, udf_property] : options_to_udf_properties) {
    options.add_options("UDFs")(option_name, udf_property.description,
                                cxxopts::value<std::string>());
  }

  options.add_options("general")("v,verbose", "Enable detailed logging")(
      "h,help", "Print this message");

  std::unordered_map<std::string, std::string> socket_paths;
  try {
    auto arguments_parse_result = options.parse(argc, argv);
    if (arguments_parse_result["help"].as<bool>()) {
      std::cout << options.help() << std::endl;
      return 0;
    }

    if (arguments_parse_result["verbose"].as<bool>()) {
      spdlog::set_level(spdlog::level::debug);
    }

    for ([[maybe_unused]] auto& [option_name, _] :
         options_to_udf_properties) {
      if (arguments_parse_result.count(option_name) == 1) {
        socket_paths[option_name] =
            arguments_parse_result[option_name].as<std::string>();
      }
    }
  } catch (const std::exception& exc) {
    std::cerr << exc.what() << std::endl;
    std::cout << options.help() << std::endl;
    return 1;
  }

  if (socket_paths.empty()) {
    std::cout << options.help() << std::endl;
    return 0;
  }

  std::vector<sdp::UnixSocketServer> udf_servers;
  for (auto& [option_name, socket_path] : socket_paths) {
    udf_servers.emplace_back(
        options_to_udf_properties.at(option_name).client_factory, socket_path,
        loop.get());
  }

  auto signal_handle = loop->resource<uvw::SignalHandle>();
  signal_handle->on<uvw::SignalEvent>(
      [&](const uvw::SignalEvent& event, uvw::SignalHandle& handle) {
        if (event.signum == SIGINT || event.signum == SIGTERM) {
          spdlog::info("Caught stop signal. Terminating...");
          for (auto& server : udf_servers) { server.stop(); }

          signal_handle->stop();
          loop->stop();
        }
      });

  signal_handle->start(SIGINT);
  signal_handle->start(SIGTERM);

  for (auto& server : udf_servers) { server.start(); }

  try {
    loop->run();
  } catch (const std::exception& exc) {
    std::cerr << exc.what() << std::endl;
    return 1;
  }

  return 0;
}
