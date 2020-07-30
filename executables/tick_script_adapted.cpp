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

  auto loop = uvw::Loop::getDefault();

  std::vector<std::string> template_strings{"*.cpu.*.percent.* host.measurement.cpu.type.field"};
  EvalNode pass_node("pass_node", loop,
                     {"127.0.0.1", 4200},
                     {
                         {"127.0.0.1", 4201}
                     },
                     std::make_shared<DataParser>(std::make_shared<GraphiteParser>(template_strings)));

  std::ofstream oss(std::string(argv[0]) + "_result.txt");
  PrintNode finalize_node("finalize_node", loop, {"127.0.0.1", 4201}, oss);

  loop->run();

  oss.close();

  return 0;
}