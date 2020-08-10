#include <chrono>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include <gandiva/tree_expr_builder.h>

#include <spdlog/spdlog.h>

#include <uvw.hpp>

#include <zmq.hpp>

#include "consumers/consumers.h"
#include "data_handlers/data_handlers.h"
#include "nodes/nodes.h"
#include "node_pipeline/node_pipeline.h"
#include "producers/producers.h"
#include "utils/parsers/graphite_parser.h"

int main(int argc, char** argv) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::flush_every(std::chrono::seconds(5));

  std::chrono::minutes win_period(7);
  std::chrono::minutes win_every(2);

  int64_t info_core_level = 100;
  int64_t warn_core_level = 90;
  int64_t crit_core_level = 85;

  int64_t info_host_level = 100;
  int64_t warn_host_level = 90;
  int64_t crit_host_level = 85;

  auto loop = uvw::Loop::getDefault();
  zmq::context_t zmq_context(1);

  std::vector<NodePipeline> pipelines;

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  auto parse_graphite_publisher_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_PUB);
  parse_graphite_publisher_socket->bind("inproc://parse_graphite_node");
  auto parse_graphite_publisher_synchronize_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_REP);
  parse_graphite_publisher_synchronize_socket->bind("inproc://parse_graphite_node_sync");
  std::shared_ptr<Consumer> parse_graphite_consumer = std::make_shared<PublisherConsumer>(TransportUtils::Publisher(
      parse_graphite_publisher_socket,
      {parse_graphite_publisher_synchronize_socket}
  ), loop);


  std::vector parse_graphite_consumers{parse_graphite_consumer};
  std::vector<std::string> template_strings{"*.cpu.*.percent.* host.measurement.cpu.type.field"};
  std::shared_ptr<Node> parse_graphite_node = std::make_shared<EvalNode>(
      "parse_graphite_node", std::move(parse_graphite_consumers), loop,
      std::make_shared<DataParser>(std::make_shared<GraphiteParser>(template_strings))
  );


  IPv4Endpoint parse_graphite_producer_endpoint{"127.0.0.1", 4200};
  std::shared_ptr<Producer> parse_graphite_producer = std::make_shared<ExternalListenerProducer>(
      parse_graphite_node, parse_graphite_producer_endpoint, loop
  );


  pipelines.emplace_back(
      std::vector{parse_graphite_consumer},
      parse_graphite_node,
      parse_graphite_producer
      );

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  auto cputime_all_filter_publisher_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_PUB);
  cputime_all_filter_publisher_socket->bind("inproc://cputime_all_filter_node");
  auto cputime_all_filter_to_cputime_all_group_by_publisher_synchronize_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_REP);
  cputime_all_filter_to_cputime_all_group_by_publisher_synchronize_socket->bind("inproc://cputime_all_filter_node_sync_to_cputime_all_group_by");
  auto cputime_all_filter_to_cputime_all_win_window_publisher_synchronize_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_REP);
  cputime_all_filter_to_cputime_all_win_window_publisher_synchronize_socket->bind("inproc://cputime_all_filter_node_sync_to_cputime_all_win_window");
  std::shared_ptr<Consumer> cputime_all_filter_consumer = std::make_shared<PublisherConsumer>(TransportUtils::Publisher(
      cputime_all_filter_publisher_socket,
      {
        cputime_all_filter_to_cputime_all_group_by_publisher_synchronize_socket,
        cputime_all_filter_to_cputime_all_win_window_publisher_synchronize_socket
      }
  ), loop);


  std::vector<gandiva::ConditionPtr> cputime_all_filter_node_conditions{
      gandiva::TreeExprBuilder::MakeCondition(gandiva::TreeExprBuilder::MakeFunction(
          "equal", {
              gandiva::TreeExprBuilder::MakeField(arrow::field("measurement", arrow::utf8())),
              gandiva::TreeExprBuilder::MakeStringLiteral("cpu")
          }, arrow::boolean()
      ))
  };
  std::vector cputime_all_filter_consumers{cputime_all_filter_consumer};
  std::shared_ptr<Node> cputime_all_filter_node = std::make_shared<EvalNode>(
      "cputime_all_filter_node", std::move(cputime_all_filter_consumers), loop,
      std::make_shared<FilterHandler>(std::move(cputime_all_filter_node_conditions))
  );


  auto cputime_all_filter_subscriber_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_SUB);
  cputime_all_filter_subscriber_socket->connect("inproc://parse_graphite_node");
  cputime_all_filter_subscriber_socket->setsockopt(ZMQ_SUBSCRIBE, "", 0);
  auto cputime_all_filter_subscriber_synchronize_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_REQ);
  cputime_all_filter_subscriber_synchronize_socket->connect("inproc://parse_graphite_node_sync");
  std::shared_ptr<Producer> cputime_all_filter_producer = std::make_shared<SubscriberProducer>(
      cputime_all_filter_node, TransportUtils::Subscriber(
          cputime_all_filter_subscriber_socket,
          cputime_all_filter_subscriber_synchronize_socket
      ), loop
  );


  pipelines.emplace_back(
      std::vector{cputime_all_filter_consumer},
      cputime_all_filter_node,
      cputime_all_filter_producer
  );

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  auto cputime_all_group_by_publisher_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_PUB);
  cputime_all_group_by_publisher_socket->bind("inproc://cputime_all_group_by_node");
  auto cputime_all_group_by_publisher_synchronize_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_REP);
  cputime_all_group_by_publisher_synchronize_socket->bind("inproc://cputime_all_group_by_node_sync");
  std::shared_ptr<Consumer> cputime_all_group_by_consumer = std::make_shared<PublisherConsumer>(TransportUtils::Publisher(
      cputime_all_group_by_publisher_socket,
      {cputime_all_group_by_publisher_synchronize_socket}
  ), loop);


  std::vector<std::string> cputime_all_grouping_columns{"host", "type"};
  std::vector cputime_all_group_by_consumers{cputime_all_group_by_consumer};
  std::shared_ptr<Node> cputime_all_group_by_node = std::make_shared<EvalNode>(
      "cputime_all_group_by_node", std::move(cputime_all_group_by_consumers), loop,
      std::make_shared<GroupHandler>(std::move(cputime_all_grouping_columns))
  );


  auto cputime_all_group_by_subscriber_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_SUB);
  cputime_all_group_by_subscriber_socket->connect("inproc://cputime_all_filter_node");
  cputime_all_group_by_subscriber_socket->setsockopt(ZMQ_SUBSCRIBE, "", 0);
  auto cputime_all_group_by_subscriber_synchronize_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_REQ);
  cputime_all_group_by_subscriber_synchronize_socket->connect("inproc://cputime_all_filter_node_sync_to_cputime_all_group_by");
  std::shared_ptr<Producer> cputime_all_group_by_producer = std::make_shared<SubscriberProducer>(
      cputime_all_group_by_node, TransportUtils::Subscriber(
          cputime_all_group_by_subscriber_socket,
          cputime_all_group_by_subscriber_synchronize_socket
      ), loop
  );


  pipelines.emplace_back(
      std::vector{cputime_all_group_by_consumer},
      cputime_all_group_by_node,
      cputime_all_group_by_producer
  );

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  auto cputime_host_last_aggregate_publisher_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_PUB);
  cputime_host_last_aggregate_publisher_socket->bind("inproc://cputime_host_last_aggregate_node");
  auto cputime_host_last_aggregate_publisher_synchronize_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_REP);
  cputime_host_last_aggregate_publisher_synchronize_socket->bind("inproc://cputime_host_last_aggregate_node_sync");
  std::shared_ptr<Consumer> cputime_host_last_aggregate_consumer = std::make_shared<PublisherConsumer>(TransportUtils::Publisher(
      cputime_host_last_aggregate_publisher_socket,
      {cputime_host_last_aggregate_publisher_synchronize_socket}
  ), loop);


  AggregateHandler::AggregateOptions cputime_host_last_options{{
                                                                   {"idle", {"last", "mean"}},
                                                                   {"interrupt", {"last", "mean"}},
                                                                   {"nice", {"last", "mean"}},
                                                                   {"softirq", {"last", "mean"}},
                                                                   {"steal", {"last", "mean"}},
                                                                   {"system", {"last", "mean"}},
                                                                   {"user", {"last", "mean"}},
                                                                   {"wait", {"last", "mean"}}
                                                               }, true};
  std::vector<std::string> cputime_host_last_grouping_columns{"host", "type"};
  std::vector cputime_host_last_aggregate_consumers{cputime_host_last_aggregate_consumer};
  std::shared_ptr<Node> cputime_host_last_aggregate_node = std::make_shared<EvalNode>(
      "cputime_host_last_aggregate_node", std::move(cputime_host_last_aggregate_consumers), loop,
      std::make_shared<AggregateHandler>(
          cputime_host_last_grouping_columns, cputime_host_last_options, "time"
      )
  );

  auto cputime_host_last_aggregate_subscriber_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_SUB);
  cputime_host_last_aggregate_subscriber_socket->connect("inproc://cputime_all_group_by_node");
  cputime_host_last_aggregate_subscriber_socket->setsockopt(ZMQ_SUBSCRIBE, "", 0);
  auto cputime_host_last_aggregate_subscriber_synchronize_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_REQ);
  cputime_host_last_aggregate_subscriber_synchronize_socket->connect("inproc://cputime_all_group_by_node_sync");
  std::shared_ptr<Producer> cputime_host_last_aggregate_producer = std::make_shared<SubscriberProducer>(
      cputime_host_last_aggregate_node, TransportUtils::Subscriber(
          cputime_host_last_aggregate_subscriber_socket,
          cputime_host_last_aggregate_subscriber_synchronize_socket
      ), loop
  );


  pipelines.emplace_back(
      std::vector{cputime_host_last_aggregate_consumer},
      cputime_host_last_aggregate_node,
      cputime_host_last_aggregate_producer
  );

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  auto cputime_host_calc_default_publisher_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_PUB);
  cputime_host_calc_default_publisher_socket->bind("inproc://cputime_host_calc_default_node");
  auto cputime_host_calc_default_publisher_synchronize_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_REP);
  cputime_host_calc_default_publisher_synchronize_socket->bind("inproc://cputime_host_calc_default_node_sync");
  std::shared_ptr<Consumer> cputime_host_calc_default_consumer = std::make_shared<PublisherConsumer>(TransportUtils::Publisher(
      cputime_host_calc_default_publisher_socket,
      {cputime_host_calc_default_publisher_synchronize_socket}
  ), loop);


  DefaultHandler::DefaultHandlerOptions cputime_host_calc_options{{},
                                                                  {
                                                                      {"info_host_level", info_host_level},
                                                                      {"warn_host_level", warn_host_level},
                                                                      {"crit_host_level", crit_host_level}
                                                                  },
                                                                  {
                                                                      {"alert-author", "@kv:qrator.net"},
                                                                      {"incident-owners", "nobody"},
                                                                      {"incident-comment", ""},
                                                                      {"alert-on", "cpu-idle-time-mean-host"}
                                                                  },
                                                                  {
                                                                      {"incident-is-expected", false}
                                                                  }};
  std::vector cputime_host_calc_default_consumers{cputime_host_calc_default_consumer};
  std::shared_ptr<Node> cputime_host_calc_default_node = std::make_shared<EvalNode>(
      "cputime_host_calc_default_node", std::move(cputime_host_calc_default_consumers), loop,
      std::make_shared<DefaultHandler>(std::move(cputime_host_calc_options))
  );


  auto cputime_host_calc_default_subscriber_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_SUB);
  cputime_host_calc_default_subscriber_socket->connect("inproc://cputime_host_last_aggregate_node");
  cputime_host_calc_default_subscriber_socket->setsockopt(ZMQ_SUBSCRIBE, "", 0);
  auto cputime_host_calc_default_subscriber_synchronize_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_REQ);
  cputime_host_calc_default_subscriber_synchronize_socket->connect("inproc://cputime_host_last_aggregate_node_sync");
  std::shared_ptr<Producer> cputime_host_calc_default_producer = std::make_shared<SubscriberProducer>(
      cputime_host_calc_default_node, TransportUtils::Subscriber(
          cputime_host_calc_default_subscriber_socket,
          cputime_host_calc_default_subscriber_synchronize_socket
      ), loop
  );


  pipelines.emplace_back(
      std::vector{cputime_host_calc_default_consumer},
      cputime_host_calc_default_node,
      cputime_host_calc_default_producer
  );

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  std::shared_ptr<Consumer> cputime_host_calc_map_consumer = std::make_shared<FilePrintConsumer>(std::string(argv[0]) + "_result_42.txt");


  gandiva::ExpressionVector cputime_host_calc_expressions{
      gandiva::TreeExprBuilder::MakeExpression(
          "less_than", {
              arrow::field("idle_mean", arrow::float64()),
              arrow::field("info_host_level", arrow::float64())
          }, arrow::field("alert_info", arrow::boolean())
      ),
      gandiva::TreeExprBuilder::MakeExpression(
          "less_than", {
              arrow::field("idle_mean", arrow::float64()),
              arrow::field("warn_host_level", arrow::float64())
          }, arrow::field("alert_warn", arrow::boolean())
      ),
      gandiva::TreeExprBuilder::MakeExpression(
          "less_than", {
              arrow::field("idle_mean", arrow::float64()),
              arrow::field("crit_host_level", arrow::float64())
          }, arrow::field("alert_crit", arrow::boolean())
      )
  };
  std::vector cputime_host_calc_map_consumers{cputime_host_calc_map_consumer};
  std::shared_ptr<Node> cputime_host_calc_map_node = std::make_shared<EvalNode>(
      "cputime_host_calc_map_node", std::move(cputime_host_calc_map_consumers), loop,
      std::make_shared<MapHandler>(std::move(cputime_host_calc_expressions))
  );


  auto cputime_host_calc_map_subscriber_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_SUB);
  cputime_host_calc_map_subscriber_socket->connect("inproc://cputime_host_calc_default_node");
  cputime_host_calc_map_subscriber_socket->setsockopt(ZMQ_SUBSCRIBE, "", 0);
  auto cputime_host_calc_map_subscriber_synchronize_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_REQ);
  cputime_host_calc_map_subscriber_synchronize_socket->connect("inproc://cputime_host_calc_default_node_sync");
  std::shared_ptr<Producer> cputime_host_calc_map_producer = std::make_shared<SubscriberProducer>(
      cputime_host_calc_map_node, TransportUtils::Subscriber(
          cputime_host_calc_map_subscriber_socket,
          cputime_host_calc_map_subscriber_synchronize_socket
      ), loop
  );


  pipelines.emplace_back(
      std::vector{cputime_host_calc_map_consumer},
      cputime_host_calc_map_node,
      cputime_host_calc_map_producer
  );

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  auto cputime_all_win_window_publisher_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_PUB);
  cputime_all_win_window_publisher_socket->bind("inproc://cputime_all_win_window_node");
  auto cputime_all_win_window_publisher_synchronize_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_REP);
  cputime_all_win_window_publisher_synchronize_socket->bind("inproc://cputime_all_win_window_node_sync");
  std::shared_ptr<Consumer> cputime_all_win_window_consumer = std::make_shared<PublisherConsumer>(TransportUtils::Publisher(
      cputime_all_win_window_publisher_socket,
      {cputime_all_win_window_publisher_synchronize_socket}
  ), loop);


  std::vector cputime_all_win_window_consumers{cputime_all_win_window_consumer};
  std::shared_ptr<Node> cputime_all_win_window_node = std::make_shared<WindowNode>(
      "cputime_all_win_window_node", std::move(cputime_all_win_window_consumers),
      std::chrono::duration_cast<std::chrono::seconds>(win_period).count(),
      std::chrono::duration_cast<std::chrono::seconds>(win_every).count(),
      "time"
  );


  auto cputime_all_win_window_subscriber_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_SUB);
  cputime_all_win_window_subscriber_socket->connect("inproc://cputime_all_filter_node");
  cputime_all_win_window_subscriber_socket->setsockopt(ZMQ_SUBSCRIBE, "", 0);
  auto cputime_all_win_window_subscriber_synchronize_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_REQ);
  cputime_all_win_window_subscriber_synchronize_socket->connect("inproc://cputime_all_filter_node_sync_to_cputime_all_win_window");
  std::shared_ptr<Producer> cputime_all_win_window_producer = std::make_shared<SubscriberProducer>(
      cputime_all_win_window_node, TransportUtils::Subscriber(
          cputime_all_win_window_subscriber_socket,
          cputime_all_win_window_subscriber_synchronize_socket
      ), loop
  );


  pipelines.emplace_back(
      std::vector{cputime_all_win_window_consumer},
      cputime_all_win_window_node,
      cputime_all_win_window_producer
  );

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  auto cputime_win_last_aggregate_publisher_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_PUB);
  cputime_win_last_aggregate_publisher_socket->bind("inproc://cputime_win_last_aggregate_node");
  auto cputime_win_last_aggregate_publisher_synchronize_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_REP);
  cputime_win_last_aggregate_publisher_synchronize_socket->bind("inproc://cputime_win_last_aggregate_node_sync");
  std::shared_ptr<Consumer> cputime_win_last_aggregate_consumer = std::make_shared<PublisherConsumer>(TransportUtils::Publisher(
      cputime_win_last_aggregate_publisher_socket,
      {cputime_win_last_aggregate_publisher_synchronize_socket}
  ), loop);


  std::vector<std::string> cputime_win_last_grouping_columns{"cpu", "host", "type"};
  std::vector cputime_win_last_aggregate_consumers{cputime_win_last_aggregate_consumer};
  std::shared_ptr<Node> cputime_win_last_aggregate_node = std::make_shared<EvalNode>(
      "cputime_win_last_aggregate_node", std::move(cputime_win_last_aggregate_consumers), loop,
      std::make_shared<AggregateHandler>(
          cputime_win_last_grouping_columns, cputime_host_last_options, "time"
      )
  );


  auto cputime_win_last_aggregate_subscriber_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_SUB);
  cputime_win_last_aggregate_subscriber_socket->connect("inproc://cputime_all_win_window_node");
  cputime_win_last_aggregate_subscriber_socket->setsockopt(ZMQ_SUBSCRIBE, "", 0);
  auto cputime_win_last_aggregate_subscriber_synchronize_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_REQ);
  cputime_win_last_aggregate_subscriber_synchronize_socket->connect("inproc://cputime_all_win_window_node_sync");
  std::shared_ptr<Producer> cputime_win_last_aggregate_producer = std::make_shared<SubscriberProducer>(
      cputime_win_last_aggregate_node, TransportUtils::Subscriber(
          cputime_win_last_aggregate_subscriber_socket,
          cputime_win_last_aggregate_subscriber_synchronize_socket
      ), loop
  );


  pipelines.emplace_back(
      std::vector{cputime_win_last_aggregate_consumer},
      cputime_win_last_aggregate_node,
      cputime_win_last_aggregate_producer
  );

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  auto cputime_win_calc_default_publisher_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_PUB);
  cputime_win_calc_default_publisher_socket->bind("inproc://cputime_win_calc_default_node");
  auto cputime_win_calc_default_publisher_synchronize_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_REP);
  cputime_win_calc_default_publisher_synchronize_socket->bind("inproc://cputime_win_calc_default_node_sync");
  std::shared_ptr<Consumer> cputime_win_calc_default_consumer = std::make_shared<PublisherConsumer>(TransportUtils::Publisher(
      cputime_win_calc_default_publisher_socket,
      {cputime_win_calc_default_publisher_synchronize_socket}
  ), loop);


  DefaultHandler::DefaultHandlerOptions cputime_win_calc_options{{},
                                                                 {
                                                                     {"info_core_level", info_core_level},
                                                                     {"warn_core_level", warn_core_level},
                                                                     {"crit_core_level", crit_core_level},
                                                                     {"win-period", std::chrono::duration_cast<std::chrono::seconds>(win_period).count()},
                                                                     {"win-every", std::chrono::duration_cast<std::chrono::seconds>(win_every).count()}
                                                                 },
                                                                 {
                                                                     {"alert-author", "@kv:qrator.net"},
                                                                     {"incident-owners", "nobody"},
                                                                     {"incident-comment", ""},
                                                                     {"alert-on", "cpu-idle-time-per-core"}
                                                                 },
                                                                 {
                                                                     {"incident-is-expected", false}
                                                                 }};  std::vector cputime_win_calc_default_consumers{cputime_win_calc_default_consumer};
  std::shared_ptr<Node> cputime_win_calc_default_node = std::make_shared<EvalNode>(
      "cputime_win_calc_default_node", std::move(cputime_win_calc_default_consumers), loop,
      std::make_shared<DefaultHandler>(std::move(cputime_win_calc_options))
  );


  auto cputime_win_calc_default_subscriber_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_SUB);
  cputime_win_calc_default_subscriber_socket->connect("inproc://cputime_win_last_aggregate_node");
  cputime_win_calc_default_subscriber_socket->setsockopt(ZMQ_SUBSCRIBE, "", 0);
  auto cputime_win_calc_default_subscriber_synchronize_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_REQ);
  cputime_win_calc_default_subscriber_synchronize_socket->connect("inproc://cputime_win_last_aggregate_node_sync");
  std::shared_ptr<Producer> cputime_win_calc_default_producer = std::make_shared<SubscriberProducer>(
      cputime_win_calc_default_node, TransportUtils::Subscriber(
          cputime_win_calc_default_subscriber_socket,
          cputime_win_calc_default_subscriber_synchronize_socket
      ), loop
  );


  pipelines.emplace_back(
      std::vector{cputime_win_calc_default_consumer},
      cputime_win_calc_default_node,
      cputime_win_calc_default_producer
  );

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  std::shared_ptr<Consumer> cputime_win_calc_map_consumer = std::make_shared<FilePrintConsumer>(std::string(argv[0]) + "_result_43.txt");


  gandiva::ExpressionVector cputime_win_calc_expressions{
      gandiva::TreeExprBuilder::MakeExpression(
          "less_than", {
              arrow::field("idle_mean", arrow::float64()),
              arrow::field("info_core_level", arrow::float64())
          }, arrow::field("alert_info", arrow::boolean())
      ),
      gandiva::TreeExprBuilder::MakeExpression(
          "less_than", {
              arrow::field("idle_mean", arrow::float64()),
              arrow::field("warn_core_level", arrow::float64())
          }, arrow::field("alert_warn", arrow::boolean())
      ),
      gandiva::TreeExprBuilder::MakeExpression(
          "less_than", {
              arrow::field("idle_mean", arrow::float64()),
              arrow::field("crit_core_level", arrow::float64())
          }, arrow::field("alert_crit", arrow::boolean())
      )
  };
  std::vector cputime_win_calc_map_consumers{cputime_win_calc_map_consumer};
  std::shared_ptr<Node> cputime_win_calc_map_node = std::make_shared<EvalNode>(
      "cputime_win_calc_map_node", std::move(cputime_win_calc_map_consumers), loop,
      std::make_shared<MapHandler>(std::move(cputime_win_calc_expressions))
  );


  auto cputime_win_calc_map_subscriber_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_SUB);
  cputime_win_calc_map_subscriber_socket->connect("inproc://cputime_win_calc_default_node");
  cputime_win_calc_map_subscriber_socket->setsockopt(ZMQ_SUBSCRIBE, "", 0);
  auto cputime_win_calc_map_subscriber_synchronize_socket = std::make_shared<zmq::socket_t>(zmq_context, ZMQ_REQ);
  cputime_win_calc_map_subscriber_synchronize_socket->connect("inproc://cputime_win_calc_default_node_sync");
  std::shared_ptr<Producer> cputime_win_calc_map_producer = std::make_shared<SubscriberProducer>(
      cputime_win_calc_map_node, TransportUtils::Subscriber(
          cputime_win_calc_map_subscriber_socket,
          cputime_win_calc_map_subscriber_synchronize_socket
      ), loop
  );


  pipelines.emplace_back(
      std::vector{cputime_win_calc_map_consumer},
      cputime_win_calc_map_node,
      cputime_win_calc_map_producer
  );

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  for (auto& pipeline : pipelines) {
    pipeline.start();
  }

  loop->run();

  return 0;
}