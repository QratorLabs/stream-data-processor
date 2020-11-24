#pragma once

#include <memory>

#include <spdlog/spdlog.h>

#include "nodes/node.h"

namespace stream_data_processor {

class Producer {
 public:
  explicit Producer(const std::shared_ptr<Node>& node) : node_(node) {}

  virtual void start() = 0;
  virtual void stop() = 0;

 protected:
  std::shared_ptr<Node> node_;
};

}  // namespace stream_data_processor
