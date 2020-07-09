#include <fstream>

#include <uvw.hpp>

#include "finalize_node.h"
#include "input_node.h"

int main() {
  auto loop = uvw::Loop::getDefault();

  std::ofstream oss("result.txt");
  FinalizeNode finalize_node(loop, {"127.0.0.1", 4250}, oss);

  InputNode pass_node(loop, {"127.0.0.1", 4240},
                        {
                            {"127.0.0.1", 4250}
                        });

  loop->run();

  return 0;
}