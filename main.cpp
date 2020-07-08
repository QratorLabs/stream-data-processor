#include <fstream>
#include <thread>

#include "finalize_node.h"
#include "input_node.h"

int main() {
  std::ofstream oss("result.txt");
  FinalizeNode finalize_node({"127.0.0.1", 4250}, oss);
  InputNode pass_node({"127.0.0.1", 4240},
                      {
                         {"127.0.0.1", 4250}
                     });

  std::thread finalize_node_thread([&]{
    finalize_node.start();
  });
  std::thread pass_node_thread([&]{
    pass_node.start();
  });

  finalize_node_thread.join();
  pass_node_thread.join();

  return 0;
}