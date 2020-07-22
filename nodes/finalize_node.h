#pragma once

#include <fstream>
#include <memory>
#include <string>

#include <arrow/api.h>

#include "uvw.hpp"

#include "node_base.h"

class FinalizeNode : public NodeBase {
 public:
  FinalizeNode(std::string name,
      const std::shared_ptr<uvw::Loop>& loop,
      const IPv4Endpoint& listen_endpoint,
      std::ofstream& ostrm);

 private:
  void configureServer();

  void writeData();
  void stop();

 private:
  std::shared_ptr<arrow::BufferBuilder> buffer_builder_;
  std::ofstream& ostrm_;
};


