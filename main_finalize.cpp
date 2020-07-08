#include <fstream>

#include "finalize_node.h"

int main() {
  std::ofstream oss("result.txt");
  FinalizeNode finalize_node({"127.0.0.1", 4250}, oss);
  finalize_node.start();
  return 0;
}