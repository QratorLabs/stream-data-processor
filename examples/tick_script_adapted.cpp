#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <gandiva/tree_expr_builder.h>

#include <spdlog/spdlog.h>

#include <uvw.hpp>

#include <zmq.hpp>

#include "consumers/consumers.h"
#include "node_pipeline/node_pipeline.h"
#include "nodes/data_handlers/data_handlers.h"
#include "nodes/nodes.h"
#include "nodes/period_handlers/serialized_period_handler.h"
#include "producers/producers.h"
#include "record_batch_handlers/record_batch_handlers.h"
#include "utils/parsers/graphite_parser.h"

int main(int argc, char** argv) {
  spdlog::set_level(spdlog::level::debug);
  spdlog::flush_every(std::chrono::seconds(5));

  std::chrono::minutes win_period(7);
  std::chrono::minutes win_every(2);

  double info_core_level = 100;
  double warn_core_level = 90;
  double crit_core_level = 85;

  double info_host_level = 100;
  double warn_host_level = 90;
  double crit_host_level = 85;

  auto loop = uvw::Loop::getDefault();
  auto zmq_context = std::make_shared<zmq::context_t>(1);

  std::unordered_map<std::string, NodePipeline> pipelines;

  GraphiteParser::GraphiteParserOptions parser_options{
      {"*.cpu.*.percent.* host.measurement.cpu.type.field"},
      "time",
      ".",
      "measurement"};
  std::shared_ptr<Node> parse_graphite_node = std::make_shared<EvalNode>(
      "parse_graphite_node",
      std::make_shared<DataParser>(
          std::make_shared<GraphiteParser>(parser_options)));

  IPv4Endpoint parse_graphite_producer_endpoint{"127.0.0.1", 4200};
  std::shared_ptr<Producer> parse_graphite_producer =
      std::make_shared<TCPProducer>(parse_graphite_node,
                                    parse_graphite_producer_endpoint,
                                    loop.get(), true);

  pipelines[parse_graphite_node->getName()] = NodePipeline();
  pipelines[parse_graphite_node->getName()].setNode(parse_graphite_node);
  pipelines[parse_graphite_node->getName()].setProducer(
      parse_graphite_producer);

  std::vector<gandiva::ConditionPtr> cputime_all_filter_node_conditions{
      gandiva::TreeExprBuilder::MakeCondition(
          gandiva::TreeExprBuilder::MakeFunction(
              "equal",
              {gandiva::TreeExprBuilder::MakeField(
                   arrow::field("measurement", arrow::utf8())),
               gandiva::TreeExprBuilder::MakeStringLiteral("cpu")},
              arrow::boolean()))};
  std::shared_ptr<Node> cputime_all_filter_node = std::make_shared<EvalNode>(
      "cputime_all_filter_node",
      std::make_shared<SerializedRecordBatchHandler>(
          std::make_shared<FilterHandler>(
              std::move(cputime_all_filter_node_conditions))));

  pipelines[cputime_all_filter_node->getName()] = NodePipeline();
  pipelines[cputime_all_filter_node->getName()].setNode(
      cputime_all_filter_node);
  pipelines[cputime_all_filter_node->getName()].subscribeTo(
      &pipelines[parse_graphite_node->getName()], loop.get(), zmq_context,
      TransportUtils::ZMQTransportType::INPROC);

  std::vector<std::string> cputime_all_grouping_columns{"host", "type"};
  std::shared_ptr<Node> cputime_all_group_by_node =
      std::make_shared<EvalNode>(
          "cputime_all_group_by_node",
          std::make_shared<SerializedRecordBatchHandler>(
              std::make_shared<GroupHandler>(
                  std::move(cputime_all_grouping_columns))));

  pipelines[cputime_all_group_by_node->getName()] = NodePipeline();
  pipelines[cputime_all_group_by_node->getName()].setNode(
      cputime_all_group_by_node);
  pipelines[cputime_all_group_by_node->getName()].subscribeTo(
      &pipelines[cputime_all_filter_node->getName()], loop.get(), zmq_context,
      TransportUtils::ZMQTransportType::INPROC);

  AggregateHandler::AggregateOptions cputime_host_last_options{
      {{"idle",
        {{AggregateHandler::AggregateFunctionEnumType::kLast, "idle.last"},
         {AggregateHandler::AggregateFunctionEnumType::kMean, "idle.mean"}}},
       {"interrupt",
        {{AggregateHandler::AggregateFunctionEnumType::kLast,
          "interrupt.last"},
         {AggregateHandler::AggregateFunctionEnumType::kMean,
          "interrupt.mean"}}},
       {"nice",
        {{AggregateHandler::AggregateFunctionEnumType::kLast, "nice.last"},
         {AggregateHandler::AggregateFunctionEnumType::kMean, "nice.mean"}}},
       {"softirq",
        {{AggregateHandler::AggregateFunctionEnumType::kLast, "softirq.last"},
         {AggregateHandler::AggregateFunctionEnumType::kMean,
          "softirq.mean"}}},
       {"steal",
        {{AggregateHandler::AggregateFunctionEnumType::kLast, "steal.last"},
         {AggregateHandler::AggregateFunctionEnumType::kMean, "steal.mean"}}},
       {"system",
        {{AggregateHandler::AggregateFunctionEnumType::kLast, "system.last"},
         {AggregateHandler::AggregateFunctionEnumType::kMean,
          "system.mean"}}},
       {"user",
        {{AggregateHandler::AggregateFunctionEnumType::kLast, "user.last"},
         {AggregateHandler::AggregateFunctionEnumType::kMean, "user.mean"}}},
       {"wait",
        {{AggregateHandler::AggregateFunctionEnumType::kLast, "wait.last"},
         {AggregateHandler::AggregateFunctionEnumType::kMean, "wait.mean"}}}},
      {AggregateHandler::AggregateFunctionEnumType::kLast, "time"}};
  std::shared_ptr<Node> cputime_host_last_aggregate_node =
      std::make_shared<EvalNode>(
          "cputime_host_last_aggregate_node",
          std::make_shared<SerializedRecordBatchHandler>(
              std::make_shared<AggregateHandler>(
                  std::move(cputime_host_last_options))));

  pipelines[cputime_host_last_aggregate_node->getName()] = NodePipeline();
  pipelines[cputime_host_last_aggregate_node->getName()].setNode(
      cputime_host_last_aggregate_node);
  pipelines[cputime_host_last_aggregate_node->getName()].subscribeTo(
      &pipelines[cputime_all_group_by_node->getName()], loop.get(),
      zmq_context, TransportUtils::ZMQTransportType::INPROC);

  DefaultHandler::DefaultHandlerOptions cputime_host_calc_options{
      {},
      {{"info_host_level", {info_host_level}},
       {"warn_host_level", {warn_host_level}},
       {"crit_host_level", {crit_host_level}}},
      {{"alert-author", {"@kv:qrator.net"}},
       {"incident-owners", {"nobody"}},
       {"incident-comment", {""}},
       {"alert-on", {"cpu-idle-time-mean-host"}}},
      {{"incident-is-expected", {false}}}};
  std::shared_ptr<Node> cputime_host_calc_default_node =
      std::make_shared<EvalNode>(
          "cputime_host_calc_default_node",
          std::make_shared<SerializedRecordBatchHandler>(
              std::make_shared<DefaultHandler>(
                  std::move(cputime_host_calc_options))));

  pipelines[cputime_host_calc_default_node->getName()] = NodePipeline();
  pipelines[cputime_host_calc_default_node->getName()].setNode(
      cputime_host_calc_default_node);
  pipelines[cputime_host_calc_default_node->getName()].subscribeTo(
      &pipelines[cputime_host_last_aggregate_node->getName()], loop.get(),
      zmq_context, TransportUtils::ZMQTransportType::INPROC);

  std::shared_ptr<Consumer> cputime_host_calc_map_consumer =
      std::make_shared<FilePrintConsumer>(std::string(argv[0]) +
                                          "_result_42.txt");

  std::vector<MapHandler::MapCase> cputime_host_calc_map_cases{
      {gandiva::TreeExprBuilder::MakeExpression(
          "less_than",
          {arrow::field("idle.mean", arrow::float64()),
           arrow::field("info_host_level", arrow::float64())},
          arrow::field("alert_info", arrow::boolean()))},
      {gandiva::TreeExprBuilder::MakeExpression(
          "less_than",
          {arrow::field("idle.mean", arrow::float64()),
           arrow::field("warn_host_level", arrow::float64())},
          arrow::field("alert_warn", arrow::boolean()))},
      {gandiva::TreeExprBuilder::MakeExpression(
          "less_than",
          {arrow::field("idle.mean", arrow::float64()),
           arrow::field("crit_host_level", arrow::float64())},
          arrow::field("alert_crit", arrow::boolean()))}};
  std::vector cputime_host_calc_map_consumers{cputime_host_calc_map_consumer};
  std::shared_ptr<Node> cputime_host_calc_map_node =
      std::make_shared<EvalNode>(
          "cputime_host_calc_map_node",
          std::move(cputime_host_calc_map_consumers),
          std::make_shared<SerializedRecordBatchHandler>(
              std::make_shared<MapHandler>(cputime_host_calc_map_cases)));

  pipelines[cputime_host_calc_map_node->getName()] = NodePipeline();
  pipelines[cputime_host_calc_map_node->getName()].addConsumer(
      cputime_host_calc_map_consumer);
  pipelines[cputime_host_calc_map_node->getName()].setNode(
      cputime_host_calc_map_node);
  pipelines[cputime_host_calc_map_node->getName()].subscribeTo(
      &pipelines[cputime_host_calc_default_node->getName()], loop.get(),
      zmq_context, TransportUtils::ZMQTransportType::INPROC);

  std::shared_ptr<Node> cputime_all_win_window_node =
      std::make_shared<PeriodNode>(
          "cputime_all_win_window_node",
          std::chrono::duration_cast<std::chrono::seconds>(win_period)
              .count(),
          std::chrono::duration_cast<std::chrono::seconds>(win_every).count(),
          "time",
          std::make_shared<SerializedPeriodHandler>(
              std::make_shared<WindowHandler>()));

  pipelines[cputime_all_win_window_node->getName()] = NodePipeline();
  pipelines[cputime_all_win_window_node->getName()].setNode(
      cputime_all_win_window_node);
  pipelines[cputime_all_win_window_node->getName()].subscribeTo(
      &pipelines[cputime_all_filter_node->getName()], loop.get(), zmq_context,
      TransportUtils::ZMQTransportType::INPROC);

  std::vector<std::string> cputime_win_grouping_columns{"cpu", "host",
                                                        "type"};
  std::shared_ptr<Node> cputime_win_group_by_node =
      std::make_shared<EvalNode>(
          "cputime_win_group_by_node",
          std::make_shared<SerializedRecordBatchHandler>(
              std::make_shared<GroupHandler>(
                  std::move(cputime_win_grouping_columns))));

  pipelines[cputime_win_group_by_node->getName()] = NodePipeline();
  pipelines[cputime_win_group_by_node->getName()].setNode(
      cputime_win_group_by_node);
  pipelines[cputime_win_group_by_node->getName()].subscribeTo(
      &pipelines[cputime_all_win_window_node->getName()], loop.get(),
      zmq_context, TransportUtils::ZMQTransportType::INPROC);

  AggregateHandler::AggregateOptions cputime_win_last_options{
      {{"idle",
        {{AggregateHandler::AggregateFunctionEnumType::kLast, "idle.last"},
         {AggregateHandler::AggregateFunctionEnumType::kMean, "idle.mean"}}},
       {"interrupt",
        {{AggregateHandler::AggregateFunctionEnumType::kLast,
          "interrupt.last"},
         {AggregateHandler::AggregateFunctionEnumType::kMean,
          "interrupt.mean"}}},
       {"nice",
        {{AggregateHandler::AggregateFunctionEnumType::kLast, "nice.last"},
         {AggregateHandler::AggregateFunctionEnumType::kMean, "nice.mean"}}},
       {"softirq",
        {{AggregateHandler::AggregateFunctionEnumType::kLast, "softirq.last"},
         {AggregateHandler::AggregateFunctionEnumType::kMean,
          "softirq.mean"}}},
       {"steal",
        {{AggregateHandler::AggregateFunctionEnumType::kLast, "steal.last"},
         {AggregateHandler::AggregateFunctionEnumType::kMean, "steal.mean"}}},
       {"system",
        {{AggregateHandler::AggregateFunctionEnumType::kLast, "system.last"},
         {AggregateHandler::AggregateFunctionEnumType::kMean,
          "system.mean"}}},
       {"user",
        {{AggregateHandler::AggregateFunctionEnumType::kLast, "user.last"},
         {AggregateHandler::AggregateFunctionEnumType::kMean, "user.mean"}}},
       {"wait",
        {{AggregateHandler::AggregateFunctionEnumType::kLast, "wait.last"},
         {AggregateHandler::AggregateFunctionEnumType::kMean, "wait.mean"}}}},
      {AggregateHandler::AggregateFunctionEnumType::kLast, "time"}};

  std::shared_ptr<Node> cputime_win_last_aggregate_node =
      std::make_shared<EvalNode>(
          "cputime_win_last_aggregate_node",
          std::make_shared<SerializedRecordBatchHandler>(
              std::make_shared<AggregateHandler>(
                  std::move(cputime_win_last_options))));

  pipelines[cputime_win_last_aggregate_node->getName()] = NodePipeline();
  pipelines[cputime_win_last_aggregate_node->getName()].setNode(
      cputime_win_last_aggregate_node);
  pipelines[cputime_win_last_aggregate_node->getName()].subscribeTo(
      &pipelines[cputime_win_group_by_node->getName()], loop.get(),
      zmq_context, TransportUtils::ZMQTransportType::INPROC);

  DefaultHandler::DefaultHandlerOptions cputime_win_calc_options{
      {},
      {{"info_core_level", {info_core_level}},
       {"warn_core_level", {warn_core_level}},
       {"crit_core_level", {crit_core_level}},
       {"win-period",
        {static_cast<double>(
            std::chrono::duration_cast<std::chrono::seconds>(win_period)
                .count())}},
       {"win-every",
        {static_cast<double>(
            std::chrono::duration_cast<std::chrono::seconds>(win_every)
                .count())}}},
      {{"alert-author", {"@kv:qrator.net"}},
       {"incident-owners", {"nobody"}},
       {"incident-comment", {""}},
       {"alert-on", {"cpu-idle-time-per-core"}}},
      {{"incident-is-expected", {false}}}};
  std::shared_ptr<Node> cputime_win_calc_default_node =
      std::make_shared<EvalNode>(
          "cputime_win_calc_default_node",
          std::make_shared<SerializedRecordBatchHandler>(
              std::make_shared<DefaultHandler>(
                  std::move(cputime_win_calc_options))));

  pipelines[cputime_win_calc_default_node->getName()] = NodePipeline();
  pipelines[cputime_win_calc_default_node->getName()].setNode(
      cputime_win_calc_default_node);
  pipelines[cputime_win_calc_default_node->getName()].subscribeTo(
      &pipelines[cputime_win_last_aggregate_node->getName()], loop.get(),
      zmq_context, TransportUtils::ZMQTransportType::INPROC);

  std::shared_ptr<Consumer> cputime_win_calc_map_consumer =
      std::make_shared<FilePrintConsumer>(std::string(argv[0]) +
                                          "_result_43.txt");

  std::vector<MapHandler::MapCase> cputime_win_calc_map_cases{
      {gandiva::TreeExprBuilder::MakeExpression(
          "less_than",
          {arrow::field("idle.mean", arrow::float64()),
           arrow::field("info_core_level", arrow::float64())},
          arrow::field("alert_info", arrow::boolean()))},
      {gandiva::TreeExprBuilder::MakeExpression(
          "less_than",
          {arrow::field("idle.mean", arrow::float64()),
           arrow::field("warn_core_level", arrow::float64())},
          arrow::field("alert_warn", arrow::boolean()))},
      {gandiva::TreeExprBuilder::MakeExpression(
          "less_than",
          {arrow::field("idle.mean", arrow::float64()),
           arrow::field("crit_core_level", arrow::float64())},
          arrow::field("alert_crit", arrow::boolean()))}};
  std::vector cputime_win_calc_map_consumers{cputime_win_calc_map_consumer};
  std::shared_ptr<Node> cputime_win_calc_map_node =
      std::make_shared<EvalNode>(
          "cputime_win_calc_map_node",
          std::move(cputime_win_calc_map_consumers),
          std::make_shared<SerializedRecordBatchHandler>(
              std::make_shared<MapHandler>(cputime_win_calc_map_cases)));

  pipelines[cputime_win_calc_map_node->getName()] = NodePipeline();
  pipelines[cputime_win_calc_map_node->getName()].addConsumer(
      cputime_win_calc_map_consumer);
  pipelines[cputime_win_calc_map_node->getName()].setNode(
      cputime_win_calc_map_node);
  pipelines[cputime_win_calc_map_node->getName()].subscribeTo(
      &pipelines[cputime_win_calc_default_node->getName()], loop.get(),
      zmq_context, TransportUtils::ZMQTransportType::INPROC);

  for (auto& [pipeline_name, pipeline] : pipelines) {
    pipeline.start();
    spdlog::info("{} pipeline was started", pipeline_name);
  }

  loop->run();

  return 0;
}
