#pragma once

#include <memory>

#include <spdlog/spdlog.h>

#include "nodes/node.h"

namespace stream_data_processor {

class Producer {
 public:
  explicit Producer(const std::shared_ptr<Node>& node) : node_(node) {}

  virtual ~Producer() = default;

  virtual void start() = 0;
  virtual void stop() = 0;

  void log(const std::string& message,
           spdlog::level::level_enum level =
               spdlog::level::level_enum::info) const {
    node_->log(message, level);
  }

  std::shared_ptr<Node> getNode() const { return node_; }

 protected:
  Producer() = default;

  Producer(const Producer& /* non-used */) = default;
  Producer& operator=(const Producer& /* non-used */) = default;

  Producer(Producer&& /* non-used */) = default;
  Producer& operator=(Producer&& /* non-used */) = default;

 private:
  std::shared_ptr<Node> node_;
};

}  // namespace stream_data_processor
