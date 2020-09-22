#pragma once

#include <memory>

#include "spdlog/spdlog.h"

#include "nodes/node.h"

class Producer {
 public:
  explicit Producer(std::shared_ptr<Node> node);

  virtual void start() = 0;
  virtual void stop() = 0;

 protected:
  std::shared_ptr<Node> node_;
};


