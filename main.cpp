#include "pass_node.h"

int main() {
  PassNode pass_node({"127.0.0.1", 4240},
      {
    {"127.0.0.1", 4250},
    {"127.0.0.1", 4251}
      });
  pass_node.start();
}