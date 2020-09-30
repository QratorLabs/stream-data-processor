#include <fstream>
#include <iostream>

#include <gandiva/tree_expr_builder.h>

#include <spdlog/spdlog.h>

#include <uvw.hpp>

#include <zmq.hpp>

#include "consumers/consumers.h"
#include "data_handlers/data_handlers.h"
#include "nodes/nodes.h"
#include "node_pipeline/node_pipeline.h"
#include "producers/producers.h"
#include "record_batch_handlers/record_batch_handlers.h"
#include "utils/parsers/csv_parser.h"

int main(int argc, char** argv) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::flush_on(spdlog::level::info);

  auto loop = uvw::Loop::getDefault();
  zmq::context_t zmq_context(1);

  std::vector<NodePipeline> pipelines;


  auto input_publisher_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_PUB);
  input_publisher_socket->bind("inproc://input_node");
  auto input_publisher_synchronize_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_REP);
  input_publisher_synchronize_socket->bind("inproc://input_node_sync");
  std::shared_ptr<Consumer> input_consumer = std::make_shared<PublisherConsumer>(TransportUtils::Publisher(
      input_publisher_socket,
      {input_publisher_synchronize_socket}
  ), loop.get());


  std::vector input_consumers{input_consumer};
  std::shared_ptr<Node> input_node = std::make_shared<EvalNode>(
      "input_node", std::move(input_consumers),
      std::make_shared<DataParser>(std::make_shared<CSVParser>())
  );


  IPv4Endpoint input_producer_endpoint{"127.0.0.1", 4200};
  std::shared_ptr<Producer> input_producer = std::make_shared<TCPProducer>(
      input_node, input_producer_endpoint, loop.get(), true
  );


  pipelines.emplace_back();
  pipelines.back().addConsumer(input_consumer);
  pipelines.back().setProducer(input_producer);
  pipelines.back().setNode(input_node);


  auto eval_publisher_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_PUB);
  eval_publisher_socket->bind("inproc://eval_node");
  auto eval_publisher_synchronize_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_REP);
  eval_publisher_synchronize_socket->bind("inproc://eval_node_sync");
  std::shared_ptr<Consumer> eval_consumer = std::make_shared<PublisherConsumer>(TransportUtils::Publisher(
      eval_publisher_socket,
      {eval_publisher_synchronize_socket}
  ), loop.get());


  auto field_ts = arrow::field("ts", arrow::timestamp(arrow::TimeUnit::SECOND));
  auto field0 = arrow::field("operand1", arrow::int64());
  auto field1 = arrow::field("operand2", arrow::int64());
  auto field2 = arrow::field("tag", arrow::utf8());
  auto schema = arrow::schema({field_ts, field0, field1, field2});

  auto field_sum = field("sum", arrow::int64());
  auto field_diff = field("diff", arrow::int64());

  auto sum_expr = gandiva::TreeExprBuilder::MakeExpression("add", {field0, field1}, field_sum);
  auto subtract_expr = gandiva::TreeExprBuilder::MakeExpression("subtract", {field0, field1}, field_diff);

  gandiva::ExpressionVector expressions{sum_expr, subtract_expr};

  std::vector eval_consumers{eval_consumer};
  std::shared_ptr<Node> eval_node = std::make_shared<EvalNode>(
      "eval_node", std::move(eval_consumers),
      std::make_shared<SerializedRecordBatchHandler>(std::make_shared<MapHandler>(std::move(expressions)))
  );


  auto eval_subscriber_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_SUB);
  eval_subscriber_socket->connect("inproc://input_node");
  eval_subscriber_socket->setsockopt(ZMQ_SUBSCRIBE, "", 0);
  auto eval_subscriber_synchronize_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_REQ);
  eval_subscriber_synchronize_socket->connect("inproc://input_node_sync");
  std::shared_ptr<Producer> eval_producer = std::make_shared<SubscriberProducer>(
      eval_node, TransportUtils::Subscriber(
          eval_subscriber_socket,
          eval_subscriber_synchronize_socket
      ), loop.get()
  );

  pipelines.emplace_back();
  pipelines.back().addConsumer(eval_consumer);
  pipelines.back().setProducer(eval_producer);
  pipelines.back().setNode(eval_node);


  std::shared_ptr<Consumer> aggregate_consumer = std::make_shared<FilePrintConsumer>(std::string(argv[0]) + "_result.txt");

  std::vector<std::string> aggregate_columns({"tag"});
  AggregateHandler::AggregateOptions aggregate_options{{{"sum", {"first", "last", "min", "max"}},
                                                        {"diff", {"first", "last", "min", "max"}}},
                                                       true};
  std::vector aggregate_consumers{aggregate_consumer};
  std::shared_ptr<Node> aggregate_node = std::make_shared<EvalNode>(
      "aggregate_node", std::move(aggregate_consumers),
      std::make_shared<SerializedRecordBatchHandler>(std::make_shared<AggregateHandler>(aggregate_columns,
                                                                                        aggregate_options,
                                                                                        "ts"))
  );


  auto aggregate_subscriber_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_SUB);
  aggregate_subscriber_socket->connect("inproc://eval_node");
  aggregate_subscriber_socket->setsockopt(ZMQ_SUBSCRIBE, "", 0);
  auto aggregate_subscriber_synchronize_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_REQ);
  aggregate_subscriber_synchronize_socket->connect("inproc://eval_node_sync");
  std::shared_ptr<Producer> aggregate_producer = std::make_shared<SubscriberProducer>(
      aggregate_node, TransportUtils::Subscriber(
          aggregate_subscriber_socket,
          aggregate_subscriber_synchronize_socket
      ), loop.get()
  );

  
  pipelines.emplace_back();
  pipelines.back().addConsumer(aggregate_consumer);
  pipelines.back().setProducer(aggregate_producer);
  pipelines.back().setNode(aggregate_node);


  for (auto& pipeline : pipelines) {
    pipeline.start();
  }

  loop->run();

  return 0;
}
