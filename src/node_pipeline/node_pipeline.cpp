#include <zmq.hpp>

#include "consumers/publisher_consumer.h"
#include "producers/subscriber_producer.h"
#include "node_pipeline.h"
#include "utils/transport_utils.h"

const std::string NodePipeline::SYNC_SUFFIX{"_sync"};

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

void NodePipeline::subscribeTo(NodePipeline &other_pipeline,
                               const std::shared_ptr<uvw::Loop> &loop,
                               const std::shared_ptr<zmq::context_t>& zmq_context,
                               TransportUtils::ZMQTransportType transport_type) {
  std::string transport_prefix;
  switch (transport_type) {
    case TransportUtils::ZMQTransportType::INPROC:
      transport_prefix = "inproc://";
      break;
    case TransportUtils::ZMQTransportType::IPC:
      transport_prefix = "ipc://";
      break;
    case TransportUtils::ZMQTransportType::TCP:
      throw std::runtime_error("Not supported yet. Please, connect pipelines via tcp manually");
    default:
      throw std::runtime_error("Unexpected ZMQ transport type");
  }

  std::string connect_prefix = transport_prefix + other_pipeline.node_->getName() + "_to_" + node_->getName();

  auto publisher_socket = std::make_shared<zmq::socket_t>(*zmq_context, ZMQ_PUB);
  publisher_socket->bind(connect_prefix);
  auto publisher_synchronize_socket = std::make_shared<zmq::socket_t>(*zmq_context, ZMQ_REP);
  publisher_synchronize_socket->bind(connect_prefix + SYNC_SUFFIX);
  std::shared_ptr<Consumer> consumer = std::make_shared<PublisherConsumer>(TransportUtils::Publisher(
      publisher_socket,
      {publisher_synchronize_socket}
  ), loop);

  other_pipeline.node_->addConsumer(consumer);
  other_pipeline.addConsumer(consumer);

  auto subscriber_socket = std::make_shared<zmq::socket_t>(*zmq_context, ZMQ_SUB);
  subscriber_socket->connect(connect_prefix);
  subscriber_socket->setsockopt(ZMQ_SUBSCRIBE, "", 0);
  auto subscriber_synchronize_socket = std::make_shared<zmq::socket_t>(*zmq_context, ZMQ_REQ);
  subscriber_synchronize_socket->connect(connect_prefix + SYNC_SUFFIX);
  std::shared_ptr<Producer> producer = std::make_shared<SubscriberProducer>(
      node_, TransportUtils::Subscriber(
          subscriber_socket,
          subscriber_synchronize_socket
      ), loop
  );

  setProducer(producer);
}
