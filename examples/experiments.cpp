#include <chrono>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <regex>

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
#include "utils/utils.h"
#include "kapacitor_udf/kapacitor_udf.h"

int main(int argc, char** argv) {
  std::regex re(R"((\S+)\((\w+)\)\s+as\s+(\S+))");
  std::smatch matches;
  std::string str("min(cpu) as min.cpu");
  if (std::regex_match(str, matches, re)) {
    for (auto& match : matches) {
      std::cout << match.str() << std::endl;
    }
  } else {
    std::cout << "Not matched" << std::endl;
  }

  return 0;
}
