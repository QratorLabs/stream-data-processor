#include <chrono>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

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

template <typename T>
class A {
 public:
  template <typename U>
  void on() {
    std::cout << "On is called\n";
  }
};

template <typename T>
class B : public A<T> {

};

int main(int argc, char** argv) {
  auto b = std::make_shared<B<int>>();
  b->template on<double>();

  return 0;
}