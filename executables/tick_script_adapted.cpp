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
  spdlog::flush_on(spdlog::level::info);

  std::chrono::seconds win_period(7);
  std::chrono::seconds win_every(2);

  int64_t info_core_level = 5;
  int64_t warn_core_level = 3;
  int64_t crit_core_level = 0;

  int64_t info_host_level = 20;
  int64_t warn_host_level = 10;
  int64_t crit_host_level = 5;

  int64_t info_load_level = 30;
  int64_t warn_load_level = 60;
  int64_t crit_load_level = 70;

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
                                {"127.0.0.1", 4202}
//                                {"127.0.0.1", 4300}
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
                                                               }};
  std::vector<std::string> cputime_host_last_grouping_columns{"host", "type"};
  EvalNode cputime_host_last_aggregate_node("cputime_host_last_aggregate_node", loop,
                                            {"127.0.0.1", 4203},
                                            {
                                                {"127.0.0.1", 4204}
                                            }, std::make_shared<AggregateHandler>(
          cputime_host_last_grouping_columns, std::move(cputime_host_last_options), "time"
      ));

  DefaultHandler::DefaultHandlerOptions cputime_host_calc_options{{
    {"info_host_level", info_host_level},
      {"warn_host_level", warn_host_level},
      {"crit_host_level", crit_host_level}
  }};
  EvalNode cputime_host_calc_default_node("cputime_host_calc_default_node", loop,
                                          {"127.0.0.1", 4204},
                                          {
                                              {"127.0.0.1", 4205}
                                          }, std::make_shared<DefaultHandler>(std::move(cputime_host_calc_options)));

//  std::vector<std::string> cputime_all_win_grouping_columns{"cpu", "host", "type"};
//  EvalNode cputime_all_win_group_node("cputime_all_win_group_node", loop,
//                                     {"127.0.0.1", 4300},
//                                     {
//                                         {"127.0.0.1", 4301}
//                                     }, std::make_shared<GroupHandler>(std::move(cputime_all_win_grouping_columns)));

  std::ofstream oss42(std::string(argv[0]) + "_result_42.txt");
  PrintNode print_node_42("print_node_42", loop, {"127.0.0.1", 4205}, oss42);

//  std::ofstream oss43(std::string(argv[0]) + "_result_43.txt");
//  PrintNode print_node_43("print_node_43", loop, {"127.0.0.1", 4301}, oss43);

  loop->run();

  oss42.close();
//  oss43.close();

  return 0;
}