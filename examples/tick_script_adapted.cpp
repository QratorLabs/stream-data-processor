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
#include "producers/producers.h"
#include "record_batch_handlers/record_batch_handlers.h"
#include "nodes/data_handlers/parsers/graphite_parser.h"

namespace sdp = stream_data_processor;

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
  zmq::context_t zmq_context(1);

  std::unordered_map<std::string, sdp::NodePipeline> pipelines;

  sdp::GraphiteParser::GraphiteParserOptions parser_options{
      {"*.cpu.*.percent.* host.measurement.cpu.type.field"},
      "time",
      ".",
      "measurement"};
  std::shared_ptr<sdp::Node> parse_graphite_node = std::make_shared<sdp::EvalNode>(
      "parse_graphite_node",
      std::make_shared<sdp::DataParser>(
          std::make_shared<sdp::GraphiteParser>(parser_options)));

  sdp::IPv4Endpoint parse_graphite_producer_endpoint{"127.0.0.1", 4200};
  std::shared_ptr<sdp::Producer> parse_graphite_producer =
      std::make_shared<sdp::TCPProducer>(parse_graphite_node,
                                    parse_graphite_producer_endpoint,
                                    loop.get(), true);

  pipelines[parse_graphite_node->getName()] = sdp::NodePipeline();
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
  std::shared_ptr<sdp::Node> cputime_all_filter_node = std::make_shared<sdp::EvalNode>(
      "cputime_all_filter_node",
      std::make_shared<sdp::SerializedRecordBatchHandler>(
          std::make_shared<sdp::FilterHandler>(
              std::move(cputime_all_filter_node_conditions))));

  pipelines[cputime_all_filter_node->getName()] = sdp::NodePipeline();
  pipelines[cputime_all_filter_node->getName()].setNode(
      cputime_all_filter_node);
  pipelines[cputime_all_filter_node->getName()].subscribeTo(
      &pipelines[parse_graphite_node->getName()], loop.get(), zmq_context,
      sdp::TransportUtils::ZMQTransportType::INPROC);

  std::vector<std::string> cputime_all_grouping_columns{"host", "type"};
  std::shared_ptr<sdp::Node> cputime_all_group_by_node =
      std::make_shared<sdp::EvalNode>(
          "cputime_all_group_by_node",
          std::make_shared<sdp::SerializedRecordBatchHandler>(
              std::make_shared<sdp::GroupHandler>(
                  std::move(cputime_all_grouping_columns))));

  pipelines[cputime_all_group_by_node->getName()] = sdp::NodePipeline();
  pipelines[cputime_all_group_by_node->getName()].setNode(
      cputime_all_group_by_node);
  pipelines[cputime_all_group_by_node->getName()].subscribeTo(
      &pipelines[cputime_all_filter_node->getName()], loop.get(), zmq_context,
      sdp::TransportUtils::ZMQTransportType::INPROC);

  sdp::AggregateHandler::AggregateOptions cputime_host_last_options{
      {{"idle",
        {{sdp::AggregateHandler::AggregateFunctionEnumType::kLast, "idle.last"},
         {sdp::AggregateHandler::AggregateFunctionEnumType::kMean, "idle.mean"}}},
       {"interrupt",
        {{sdp::AggregateHandler::AggregateFunctionEnumType::kLast,
          "interrupt.last"},
         {sdp::AggregateHandler::AggregateFunctionEnumType::kMean,
          "interrupt.mean"}}},
       {"nice",
        {{sdp::AggregateHandler::AggregateFunctionEnumType::kLast, "nice.last"},
         {sdp::AggregateHandler::AggregateFunctionEnumType::kMean, "nice.mean"}}},
       {"softirq",
        {{sdp::AggregateHandler::AggregateFunctionEnumType::kLast, "softirq.last"},
         {sdp::AggregateHandler::AggregateFunctionEnumType::kMean,
          "softirq.mean"}}},
       {"steal",
        {{sdp::AggregateHandler::AggregateFunctionEnumType::kLast, "steal.last"},
         {sdp::AggregateHandler::AggregateFunctionEnumType::kMean, "steal.mean"}}},
       {"system",
        {{sdp::AggregateHandler::AggregateFunctionEnumType::kLast, "system.last"},
         {sdp::AggregateHandler::AggregateFunctionEnumType::kMean,
          "system.mean"}}},
       {"user",
        {{sdp::AggregateHandler::AggregateFunctionEnumType::kLast, "user.last"},
         {sdp::AggregateHandler::AggregateFunctionEnumType::kMean, "user.mean"}}},
       {"wait",
        {{sdp::AggregateHandler::AggregateFunctionEnumType::kLast, "wait.last"},
         {sdp::AggregateHandler::AggregateFunctionEnumType::kMean, "wait.mean"}}}},
      {sdp::AggregateHandler::AggregateFunctionEnumType::kLast, "time"}};
  std::shared_ptr<sdp::Node> cputime_host_last_aggregate_node =
      std::make_shared<sdp::EvalNode>(
          "cputime_host_last_aggregate_node",
          std::make_shared<sdp::SerializedRecordBatchHandler>(
              std::make_shared<sdp::AggregateHandler>(
                  std::move(cputime_host_last_options))));

  pipelines[cputime_host_last_aggregate_node->getName()] = sdp::NodePipeline();
  pipelines[cputime_host_last_aggregate_node->getName()].setNode(
      cputime_host_last_aggregate_node);
  pipelines[cputime_host_last_aggregate_node->getName()].subscribeTo(
      &pipelines[cputime_all_group_by_node->getName()], loop.get(),
      zmq_context, sdp::TransportUtils::ZMQTransportType::INPROC);

  sdp::DefaultHandler::DefaultHandlerOptions cputime_host_calc_options{
      {},
      {{"info_host_level", {info_host_level}},
       {"warn_host_level", {warn_host_level}},
       {"crit_host_level", {crit_host_level}}},
      {{"alert-author", {"@kv:qrator.net"}},
       {"incident-owners", {"nobody"}},
       {"incident-comment", {""}},
       {"alert-on", {"cpu-idle-time-mean-host"}}},
      {{"incident-is-expected", {false}}}};
  std::shared_ptr<sdp::Node> cputime_host_calc_default_node =
      std::make_shared<sdp::EvalNode>(
          "cputime_host_calc_default_node",
          std::make_shared<sdp::SerializedRecordBatchHandler>(
              std::make_shared<sdp::DefaultHandler>(
                  std::move(cputime_host_calc_options))));

  pipelines[cputime_host_calc_default_node->getName()] = sdp::NodePipeline();
  pipelines[cputime_host_calc_default_node->getName()].setNode(
      cputime_host_calc_default_node);
  pipelines[cputime_host_calc_default_node->getName()].subscribeTo(
      &pipelines[cputime_host_last_aggregate_node->getName()], loop.get(),
      zmq_context, sdp::TransportUtils::ZMQTransportType::INPROC);

  std::shared_ptr<sdp::Consumer> cputime_host_calc_map_consumer =
      std::make_shared<sdp::FilePrintConsumer>(std::string(argv[0]) +
                                          "_result_42.txt");

  std::vector<sdp::MapHandler::MapCase> cputime_host_calc_map_cases{
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
  std::shared_ptr<sdp::Node> cputime_host_calc_map_node =
      std::make_shared<sdp::EvalNode>(
          "cputime_host_calc_map_node",
          std::move(cputime_host_calc_map_consumers),
          std::make_shared<sdp::SerializedRecordBatchHandler>(
              std::make_shared<sdp::MapHandler>(cputime_host_calc_map_cases)));

  pipelines[cputime_host_calc_map_node->getName()] = sdp::NodePipeline();
  pipelines[cputime_host_calc_map_node->getName()].addConsumer(
      cputime_host_calc_map_consumer);
  pipelines[cputime_host_calc_map_node->getName()].setNode(
      cputime_host_calc_map_node);
  pipelines[cputime_host_calc_map_node->getName()].subscribeTo(
      &pipelines[cputime_host_calc_default_node->getName()], loop.get(),
      zmq_context, sdp::TransportUtils::ZMQTransportType::INPROC);

  sdp::WindowHandler::WindowOptions window_options{
    std::chrono::duration_cast<std::chrono::seconds>(win_period),
    std::chrono::duration_cast<std::chrono::seconds>(win_every),
    false
  };

  std::shared_ptr<sdp::Node> cputime_all_win_window_node =
      std::make_shared<sdp::EvalNode>(
          "cputime_all_win_window_node",
          std::make_shared<sdp::SerializedRecordBatchHandler>(
              std::make_shared<sdp::WindowHandler>(std::move(window_options))));

  pipelines[cputime_all_win_window_node->getName()] = sdp::NodePipeline();
  pipelines[cputime_all_win_window_node->getName()].setNode(
      cputime_all_win_window_node);
  pipelines[cputime_all_win_window_node->getName()].subscribeTo(
      &pipelines[cputime_all_filter_node->getName()], loop.get(), zmq_context,
      sdp::TransportUtils::ZMQTransportType::INPROC);

  std::vector<std::string> cputime_win_grouping_columns{"cpu", "host",
                                                        "type"};
  std::shared_ptr<sdp::Node> cputime_win_group_by_node =
      std::make_shared<sdp::EvalNode>(
          "cputime_win_group_by_node",
          std::make_shared<sdp::SerializedRecordBatchHandler>(
              std::make_shared<sdp::GroupHandler>(
                  std::move(cputime_win_grouping_columns))));

  pipelines[cputime_win_group_by_node->getName()] = sdp::NodePipeline();
  pipelines[cputime_win_group_by_node->getName()].setNode(
      cputime_win_group_by_node);
  pipelines[cputime_win_group_by_node->getName()].subscribeTo(
      &pipelines[cputime_all_win_window_node->getName()], loop.get(),
      zmq_context, sdp::TransportUtils::ZMQTransportType::INPROC);

  sdp::AggregateHandler::AggregateOptions cputime_win_last_options{
      {{"idle",
        {{sdp::AggregateHandler::AggregateFunctionEnumType::kLast, "idle.last"},
         {sdp::AggregateHandler::AggregateFunctionEnumType::kMean, "idle.mean"}}},
       {"interrupt",
        {{sdp::AggregateHandler::AggregateFunctionEnumType::kLast,
          "interrupt.last"},
         {sdp::AggregateHandler::AggregateFunctionEnumType::kMean,
          "interrupt.mean"}}},
       {"nice",
        {{sdp::AggregateHandler::AggregateFunctionEnumType::kLast, "nice.last"},
         {sdp::AggregateHandler::AggregateFunctionEnumType::kMean, "nice.mean"}}},
       {"softirq",
        {{sdp::AggregateHandler::AggregateFunctionEnumType::kLast, "softirq.last"},
         {sdp::AggregateHandler::AggregateFunctionEnumType::kMean,
          "softirq.mean"}}},
       {"steal",
        {{sdp::AggregateHandler::AggregateFunctionEnumType::kLast, "steal.last"},
         {sdp::AggregateHandler::AggregateFunctionEnumType::kMean, "steal.mean"}}},
       {"system",
        {{sdp::AggregateHandler::AggregateFunctionEnumType::kLast, "system.last"},
         {sdp::AggregateHandler::AggregateFunctionEnumType::kMean,
          "system.mean"}}},
       {"user",
        {{sdp::AggregateHandler::AggregateFunctionEnumType::kLast, "user.last"},
         {sdp::AggregateHandler::AggregateFunctionEnumType::kMean, "user.mean"}}},
       {"wait",
        {{sdp::AggregateHandler::AggregateFunctionEnumType::kLast, "wait.last"},
         {sdp::AggregateHandler::AggregateFunctionEnumType::kMean, "wait.mean"}}}},
      {sdp::AggregateHandler::AggregateFunctionEnumType::kLast, "time"}};

  std::shared_ptr<sdp::Node> cputime_win_last_aggregate_node =
      std::make_shared<sdp::EvalNode>(
          "cputime_win_last_aggregate_node",
          std::make_shared<sdp::SerializedRecordBatchHandler>(
              std::make_shared<sdp::AggregateHandler>(
                  std::move(cputime_win_last_options))));

  pipelines[cputime_win_last_aggregate_node->getName()] = sdp::NodePipeline();
  pipelines[cputime_win_last_aggregate_node->getName()].setNode(
      cputime_win_last_aggregate_node);
  pipelines[cputime_win_last_aggregate_node->getName()].subscribeTo(
      &pipelines[cputime_win_group_by_node->getName()], loop.get(),
      zmq_context, sdp::TransportUtils::ZMQTransportType::INPROC);

  sdp::DefaultHandler::DefaultHandlerOptions cputime_win_calc_options{
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
  std::shared_ptr<sdp::Node> cputime_win_calc_default_node =
      std::make_shared<sdp::EvalNode>(
          "cputime_win_calc_default_node",
          std::make_shared<sdp::SerializedRecordBatchHandler>(
              std::make_shared<sdp::DefaultHandler>(
                  std::move(cputime_win_calc_options))));

  pipelines[cputime_win_calc_default_node->getName()] = sdp::NodePipeline();
  pipelines[cputime_win_calc_default_node->getName()].setNode(
      cputime_win_calc_default_node);
  pipelines[cputime_win_calc_default_node->getName()].subscribeTo(
      &pipelines[cputime_win_last_aggregate_node->getName()], loop.get(),
      zmq_context, sdp::TransportUtils::ZMQTransportType::INPROC);

  std::shared_ptr<sdp::Consumer> cputime_win_calc_map_consumer =
      std::make_shared<sdp::FilePrintConsumer>(std::string(argv[0]) +
                                          "_result_43.txt");

  std::vector<sdp::MapHandler::MapCase> cputime_win_calc_map_cases{
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
  std::shared_ptr<sdp::Node> cputime_win_calc_map_node =
      std::make_shared<sdp::EvalNode>(
          "cputime_win_calc_map_node",
          std::move(cputime_win_calc_map_consumers),
          std::make_shared<sdp::SerializedRecordBatchHandler>(
              std::make_shared<sdp::MapHandler>(cputime_win_calc_map_cases)));

  pipelines[cputime_win_calc_map_node->getName()] = sdp::NodePipeline();
  pipelines[cputime_win_calc_map_node->getName()].addConsumer(
      cputime_win_calc_map_consumer);
  pipelines[cputime_win_calc_map_node->getName()].setNode(
      cputime_win_calc_map_node);
  pipelines[cputime_win_calc_map_node->getName()].subscribeTo(
      &pipelines[cputime_win_calc_default_node->getName()], loop.get(),
      zmq_context, sdp::TransportUtils::ZMQTransportType::INPROC);

  for (auto& [pipeline_name, pipeline] : pipelines) {
    pipeline.start();
    spdlog::info("{} pipeline was started", pipeline_name);
  }

  loop->run();

  return 0;
}
