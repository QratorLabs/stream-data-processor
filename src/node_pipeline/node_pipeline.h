#pragma once

#include <memory>
#include <vector>

#include <uvw.hpp>

#include "consumers/consumer.h"
#include "nodes/node.h"
#include "producers/producer.h"
#include "utils/transport_utils.h"

namespace stream_data_processor {

using transport_utils::TransportUtils;

class NodePipeline {
 public:
  NodePipeline() = default;

  template <typename ConsumerVectorType>
  NodePipeline(ConsumerVectorType&& consumers, std::shared_ptr<Node> node,
               std::shared_ptr<Producer> producer)
      : consumers_(std::forward<ConsumerVectorType>(consumers)),
        node_(std::move(node)),
        producer_(std::move(producer)) {}

  void addConsumer(std::shared_ptr<Consumer> consumer);
  void setNode(std::shared_ptr<Node> node);
  void setProducer(std::shared_ptr<Producer> producer);

  void start();

  void subscribeTo(NodePipeline* other_pipeline, uvw::Loop* loop,
                   zmq::context_t& zmq_context,
                   TransportUtils::ZMQTransportType transport_type =
                       TransportUtils::ZMQTransportType::INPROC);

 private:
  static const std::string SYNC_SUFFIX;

  std::vector<std::shared_ptr<Consumer>> consumers_;
  std::shared_ptr<Node> node_{nullptr};
  std::shared_ptr<Producer> producer_{nullptr};
};

}  // namespace stream_data_processor
