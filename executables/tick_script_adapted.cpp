#include <chrono>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include <gandiva/tree_expr_builder.h>

#include <spdlog/spdlog.h>

#include <uvw.hpp>

#include "data_handlers/data_handlers.h"
#include "nodes/nodes.h"
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

  std::vector<std::string> template_strings{"*.cpu.*.percent.* host.measurement.cpu.type.field"};
  EvalNode parse_graphite_node("parse_graphite_node", loop,
                               {"127.0.0.1", 4200},
                               {
                                   {"127.0.0.1", 4201}
                               },
                               std::make_shared<DataParser>(std::make_shared<GraphiteParser>(template_strings)),
                                   true);

  std::vector<gandiva::ConditionPtr> cputime_all_filter_node_conditions{
      gandiva::TreeExprBuilder::MakeCondition(gandiva::TreeExprBuilder::MakeFunction(
          "equal", {
            gandiva::TreeExprBuilder::MakeField(arrow::field("measurement", arrow::utf8())),
            gandiva::TreeExprBuilder::MakeStringLiteral("cpu")
          }, arrow::boolean()
          ))
  };
  EvalNode cputime_all_filter_node("cputime_all_filter_node", loop,
                                   {"127.0.0.1", 4201},
                                   {
                                {"127.0.0.1", 4202},
                                {"127.0.0.1", 4300}
                            },
                                   std::make_shared<FilterHandler>(std::move(cputime_all_filter_node_conditions)));

  std::vector<std::string> cputime_all_grouping_columns{"host", "type"};
  EvalNode cputime_all_group_by_node("cputime_all_group_by_node", loop,
                                     {"127.0.0.1", 4202},
                                     {
                                         {"127.0.0.1", 4203}
                                     }, std::make_shared<GroupHandler>(std::move(cputime_all_grouping_columns)));

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
  EvalNode cputime_host_last_aggregate_node("cputime_host_last_aggregate_node", loop,
                                            {"127.0.0.1", 4203},
                                            {
                                                {"127.0.0.1", 4204}
                                            }, std::make_shared<AggregateHandler>(
          cputime_host_last_grouping_columns, cputime_host_last_options, "time"
      ));

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
  EvalNode cputime_host_calc_default_node("cputime_host_calc_default_node", loop,
                                          {"127.0.0.1", 4204},
                                          {
                                              {"127.0.0.1", 4205}
                                          }, std::make_shared<DefaultHandler>(std::move(cputime_host_calc_options)));

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
  EvalNode cputime_host_calc_map_node("cputime_host_calc_map_node", loop,
                                     {"127.0.0.1", 4205},
                                     {
                                         {"127.0.0.1", 4206}
                                     }, std::make_shared<MapHandler>(std::move(cputime_host_calc_expressions)));

  WindowNode cputime_all_win_window_node("cputime_all_win_window_node", loop,
                                         {"127.0.0.1", 4300},
                                         {
                                             {"127.0.0.1", 4301}
                                         },
                                         std::chrono::duration_cast<std::chrono::seconds>(win_period).count(),
                                         std::chrono::duration_cast<std::chrono::seconds>(win_every).count(),
                                             "time");

  std::vector<std::string> cputime_win_last_grouping_columns{"cpu", "host", "type"};
  EvalNode cputime_win_last_aggregate_node("cputime_win_last_aggregate_node", loop,
                                            {"127.0.0.1", 4301},
                                            {
                                                {"127.0.0.1", 4302}
                                            }, std::make_shared<AggregateHandler>(
          cputime_win_last_grouping_columns, cputime_host_last_options, "time"
      ));

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
                                                                 }};
  EvalNode cputime_win_calc_default_node("cputime_win_calc_default_node", loop,
                                          {"127.0.0.1", 4302},
                                          {
                                              {"127.0.0.1", 4303}
                                          }, std::make_shared<DefaultHandler>(std::move(cputime_win_calc_options)));

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
  EvalNode cputime_win_calc_map_node("cputime_win_calc_map_node", loop,
                                     {"127.0.0.1", 4303},
                                     {
                                         {"127.0.0.1", 4304}
                                     }, std::make_shared<MapHandler>(std::move(cputime_win_calc_expressions)));

  std::ofstream oss42(std::string(argv[0]) + "_result_42.txt");
  PrintNode print_node_42("print_node_42", loop, {"127.0.0.1", 4206}, oss42);

  std::ofstream oss43(std::string(argv[0]) + "_result_43.txt");
  PrintNode print_node_43("print_node_43", loop, {"127.0.0.1", 4304}, oss43);

  loop->run();

  oss42.close();
  oss43.close();

  return 0;
}