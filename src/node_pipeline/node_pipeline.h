#pragma once

#include <memory>
#include <vector>

#include "consumers/consumer.h"
#include "nodes/node.h"
#include "producers/producer.h"

class NodePipeline {
 public:
  NodePipeline() = default;

  template <typename U>
  NodePipeline(U&& consumers, std::shared_ptr<Node> node, std::shared_ptr<Producer> producer)
      : consumers_(std::forward<U>(consumers))
      , node_(std::move(node))
      , producer_(std::move(producer)) {

  }

  void addConsumer(const std::shared_ptr<Consumer>& consumer);
  void setNode(const std::shared_ptr<Node>& node);
  void setProducer(const std::shared_ptr<Producer>& producer);

  void start();

 private:
  std::vector<std::shared_ptr<Consumer>> consumers_;
  std::shared_ptr<Node> node_;
  std::shared_ptr<Producer> producer_;
};


