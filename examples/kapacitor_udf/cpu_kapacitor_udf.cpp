#include <chrono>
#include <csignal>
#include <memory>
#include <vector>

#include <spdlog/spdlog.h>

#include <uvw.hpp>

#include "kapacitor_udf/kapacitor_udf.h"
#include "record_batch_handlers/record_batch_handlers.h"
#include "server/unix_socket_server.h"
#include "server/unix_socket_client.h"


class CpuAggregateUDFAgentClientFactory : public UnixSocketClientFactory {
 public:
  std::shared_ptr<UnixSocketClient> createClient(const std::shared_ptr<uvw::PipeHandle>& pipe_handle) override {
    auto agent = std::make_shared<SocketBasedUDFAgent>(pipe_handle, pipe_handle);

    AggregateHandler::AggregateOptions cputime_host_last_options{
        {
            {"idle", {
              {AggregateHandler::AggregateFunctionEnumType::kLast, "idle.last"},
              {AggregateHandler::AggregateFunctionEnumType::kMean, "idle.mean"}
            }},
            {"interrupt", {
                {AggregateHandler::AggregateFunctionEnumType::kLast, "interrupt.last"},
                {AggregateHandler::AggregateFunctionEnumType::kMean, "interrupt.mean"}
            }},
            {"nice", {
                {AggregateHandler::AggregateFunctionEnumType::kLast, "nice.last"},
                {AggregateHandler::AggregateFunctionEnumType::kMean, "nice.mean"}
            }},
            {"softirq", {
                {AggregateHandler::AggregateFunctionEnumType::kLast, "softirq.last"},
                {AggregateHandler::AggregateFunctionEnumType::kMean, "softirq.mean"}
            }},
            {"steal", {
                {AggregateHandler::AggregateFunctionEnumType::kLast, "steal.last"},
                {AggregateHandler::AggregateFunctionEnumType::kMean, "steal.mean"}
            }},
            {"system", {
                {AggregateHandler::AggregateFunctionEnumType::kLast, "system.last"},
                {AggregateHandler::AggregateFunctionEnumType::kMean, "system.mean"}
            }},
            {"user", {
                {AggregateHandler::AggregateFunctionEnumType::kLast, "user.last"},
                {AggregateHandler::AggregateFunctionEnumType::kMean, "user.mean"}
            }},
            {"wait", {
                {AggregateHandler::AggregateFunctionEnumType::kLast, "wait.last"},
                {AggregateHandler::AggregateFunctionEnumType::kMean, "wait.mean"}
            }}
        }, { }, "time", true, {AggregateHandler::AggregateFunctionEnumType::kLast, "time"}
    };
    std::vector<std::shared_ptr<RecordBatchHandler>> handlers_pipeline{
        std::make_shared<AggregateHandler>(
            std::move(cputime_host_last_options)
        )
    };

    DataConverter::PointsToRecordBatchesConversionOptions to_record_batches_options{
        "time",
        "measurement"
    };
    DataConverter::RecordBatchesToPointsConversionOptions to_points_options{
        "to_time",
        "measurement",
        {"cpu", "host", "type"}
    };

    std::shared_ptr<RequestHandler> handler = std::make_shared<BatchToStreamRequestHandler>(
        agent, std::move(handlers_pipeline), to_record_batches_options, to_points_options
        );

    agent->setHandler(handler);
    return std::make_shared<AgentClient>(agent);
  }
};


int main(int argc, char** argv) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::flush_every(std::chrono::seconds(5));

  if (argc < 2) {
    spdlog::error("Unix socket path not provided");
    return 1;
  }

  auto loop = uvw::Loop::getDefault();
  std::string socket_path(argv[1]);

  UnixSocketServer server(std::make_shared<CpuAggregateUDFAgentClientFactory>(), socket_path, loop.get());

  auto signal_handle = loop->resource<uvw::SignalHandle>();
  signal_handle->on<uvw::SignalEvent>([&](const uvw::SignalEvent& event, uvw::SignalHandle& handle) {
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
