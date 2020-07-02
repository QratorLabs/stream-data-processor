#include "server.h"

int main() {
  auto loop = uvw::Loop::getDefault();
  Server server(loop);
  server.start("127.0.0.1", 4243);
  loop->run();
}