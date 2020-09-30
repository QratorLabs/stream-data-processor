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
            {"idle", {"last", "mean"}},
            {"interrupt", {"last", "mean"}},
            {"nice", {"last", "mean"}},
            {"softirq", {"last", "mean"}},
            {"steal", {"last", "mean"}},
            {"system", {"last", "mean"}},
            {"user", {"last", "mean"}},
            {"wait", {"last", "mean"}}
        }, true
    };
    std::vector<std::string> cputime_host_last_grouping_columns{ };
    std::vector<std::shared_ptr<RecordBatchHandler>> handlers_pipeline{
        std::make_shared<AggregateHandler>(
            cputime_host_last_grouping_columns, cputime_host_last_options, "time"
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

  auto loop = uvw::Loop::getDefault();
  std::string socket_path("/var/run/cpu.sock");

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
