#include <spdlog/spdlog.h>

#include <uvw.hpp>

#include "consumers/consumers.h"
#include "nodes/data_handlers/data_handlers.h"
#include "node_pipeline/node_pipeline.h"
#include "nodes/nodes.h"
#include "nodes/period_handlers/serialized_period_handler.h"
#include "producers/producers.h"
#include "record_batch_handlers/record_batch_handlers.h"
#include "utils/parsers/csv_parser.h"

int main(int argc, char** argv) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::flush_on(spdlog::level::info);

  auto zmq_context = zmq::context_t(1);
  auto loop = uvw::Loop::getDefault();
  std::vector<NodePipeline> pipelines;

  auto input_publisher_socket =
      std::make_shared<zmq::socket_t>(zmq_context, ZMQ_PUB);
  input_publisher_socket->bind("inproc://input_node");
  auto input_publisher_synchronize_socket =
      std::make_shared<zmq::socket_t>(zmq_context, ZMQ_REP);
  input_publisher_synchronize_socket->bind("inproc://input_node_sync");
  std::shared_ptr<Consumer> input_consumer =
      std::make_shared<PublisherConsumer>(
          TransportUtils::Publisher(input_publisher_socket,
                                    {input_publisher_synchronize_socket}),
          loop.get());

  std::vector input_consumers{input_consumer};
  std::shared_ptr<Node> input_node = std::make_shared<EvalNode>(
      "input_node", std::move(input_consumers),
      std::make_shared<DataParser>(std::make_shared<CSVParser>()));

  IPv4Endpoint input_producer_endpoint{"127.0.0.1", 4200};
  std::shared_ptr<Producer> input_producer = std::make_shared<TCPProducer>(
      input_node, input_producer_endpoint, loop.get(), true);

  pipelines.emplace_back();
  pipelines.back().addConsumer(input_consumer);
  pipelines.back().setProducer(input_producer);
  pipelines.back().setNode(input_node);

  std::shared_ptr<Consumer> window_consumer =
      std::make_shared<FilePrintConsumer>(std::string(argv[0]) +
                                          "_result.txt");

  std::vector window_consumers{window_consumer};
  std::shared_ptr<Node> window_node = std::make_shared<PeriodNode>(
      "window_node", std::move(window_consumers), 30, 10, "ts",
      std::make_shared<SerializedPeriodHandler>(
          std::make_shared<WindowHandler>()));

  auto window_subscriber_socket =
      std::make_shared<zmq::socket_t>(zmq_context, ZMQ_SUB);
  window_subscriber_socket->connect("inproc://input_node");
  window_subscriber_socket->setsockopt(ZMQ_SUBSCRIBE, "", 0);
  auto window_subscriber_synchronize_socket =
      std::make_shared<zmq::socket_t>(zmq_context, ZMQ_REQ);
  window_subscriber_synchronize_socket->connect("inproc://input_node_sync");
  std::shared_ptr<Producer> window_producer =
      std::make_shared<SubscriberProducer>(
          window_node,
          TransportUtils::Subscriber(window_subscriber_socket,
                                     window_subscriber_synchronize_socket),
          loop.get());

  pipelines.emplace_back(std::vector{window_consumer}, window_node,
                         window_producer);

  for (auto& pipeline : pipelines) { pipeline.start(); }

  loop->run();

  return 0;
}
