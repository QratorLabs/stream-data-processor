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

  void log(const std::string& message,
           spdlog::level::level_enum level =
               spdlog::level::level_enum::info) const {
    node_->log(message, level);
  }

  std::shared_ptr<Node> getNode() const { return node_; }

 private:
  std::shared_ptr<Node> node_;
};

}  // namespace stream_data_processor
