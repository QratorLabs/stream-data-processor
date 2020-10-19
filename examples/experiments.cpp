#include <chrono>
#include <fstream>
#include <iostream>
#include <memory>
#include <regex>
#include <string>
#include <vector>

#include <gandiva/tree_expr_builder.h>

#include <spdlog/spdlog.h>

#include <uvw.hpp>

#include <zmq.hpp>

#include "consumers/consumers.h"
#include "nodes/data_handlers/data_handlers.h"
#include "kapacitor_udf/kapacitor_udf.h"
#include "node_pipeline/node_pipeline.h"
#include "nodes/nodes.h"
#include "producers/producers.h"
#include "utils/parsers/graphite_parser.h"
#include "utils/utils.h"

int main(int argc, char** argv) {
  auto loop = uvw::Loop::getDefault();
  ChildProcessBasedUDFAgent agent(loop.get());

  return 0;
}
