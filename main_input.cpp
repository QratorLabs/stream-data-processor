#include "input_node.h"

int main() {
  InputNode pass_node({"127.0.0.1", 4240},
                      {
                         {"127.0.0.1", 4250}
                     });
  pass_node.start();

  return 0;
}