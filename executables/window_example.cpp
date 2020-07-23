#include <fstream>

#include <gandiva/tree_expr_builder.h>

#include <spdlog/spdlog.h>

#include <uvw.hpp>

#include "data_handlers/data_handlers.h"
#include "nodes/nodes.h"

int main(int argc, char** argv) {
  spdlog::set_level(spdlog::level::debug);

  auto loop = uvw::Loop::getDefault();

  std::ofstream oss(std::string(argv[0]) + "_result.txt");
  FinalizeNode finalize_node("finalize_node", loop, {"127.0.0.1", 4250}, oss);

  WindowNode window_node("window_node", loop, {"127.0.0.1", 4241},
                         {
                             {"127.0.0.1", 4250}
                         },
                         30, 10, "ts");

  EvalNode input_node("input_node", loop,
                      {"127.0.0.1", 4240},
                      {
                         {"127.0.0.1", 4241}
                     },
                      std::make_shared<CSVToRecordBatchesConverter>());

  loop->run();

  oss.close();

  return 0;
}