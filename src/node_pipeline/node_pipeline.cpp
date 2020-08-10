#include "node_pipeline.h"

void NodePipeline::addConsumer(const std::shared_ptr<Consumer> &consumer) {
  consumers_.push_back(consumer);
}

void NodePipeline::setNode(const std::shared_ptr<Node> &node) {
  node_ = node;
}

void NodePipeline::setProducer(const std::shared_ptr<Producer> &producer) {
  producer_ = producer;
}

void NodePipeline::start() {
  producer_->start();
  node_->start();
  for (auto& consumer : consumers_) {
    consumer->start();
  }
}
