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

  std::chrono::minutes win_period(7);
  std::chrono::minutes win_every(2);

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

//  EvalNode load_filter_node("load_filter_node", loop,
//                            {"127.0.0.1", 4201},
//                            {
//                                {"127.0.0.1", 4202}
//                            },
//                            std::make_shared<FilterHandler>())

  std::ofstream oss(std::string(argv[0]) + "_result.txt");
  PrintNode print_node("print_node", loop, {"127.0.0.1", 4201}, oss);

  loop->run();

  oss.close();

  return 0;
}