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
  auto loop = uvw::Loop::getDefault();
  ChildProcessBasedUDFAgent agent(loop.get());

  return 0;
}
