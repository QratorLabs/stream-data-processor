#pragma once

#include <fstream>
#include <memory>

#include <arrow/api.h>

#include "uvw.hpp"

#include "utils.h"

class FinalizeNode {
 public:
  FinalizeNode(std::shared_ptr<uvw::Loop> loop, const IPv4Endpoint& listen_endpoint, std::ofstream& ostrm);

 private:
  void configureServer(const IPv4Endpoint& endpoint);

  void writeData();

  void stop();

 private:
  std::shared_ptr<uvw::Loop> loop_;
  std::shared_ptr<uvw::TCPHandle> server_;
  std::shared_ptr<arrow::BufferBuilder> buffer_builder_;
  std::ofstream& ostrm_;
};


