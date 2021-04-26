#pragma once

#include "kapacitor_udf.h"
#include "server/unix_socket_client.h"

#include <uvw.hpp>

namespace stream_data_processor {
namespace kapacitor_udf {

class BatchAggregateUDFAgentClientFactory : public UnixSocketClientFactory {
 public:
  std::shared_ptr<UnixSocketClient> createClient(
      const std::shared_ptr<uvw::PipeHandle>& pipe_handle) override {
    auto agent =
        std::make_shared<SocketBasedUDFAgent>(pipe_handle, pipe_handle);

    std::shared_ptr<RequestHandler> handler =
        std::make_shared<BatchAggregateRequestHandler>(agent.get());

    agent->setHandler(handler);
    return std::make_shared<AgentClient>(agent);
  }
};

class StreamAggregateUDFAgentClientFactory : public UnixSocketClientFactory {
 public:
  explicit StreamAggregateUDFAgentClientFactory(uvw::Loop* loop)
      : loop_(loop) {}

  std::shared_ptr<UnixSocketClient> createClient(
      const std::shared_ptr<uvw::PipeHandle>& pipe_handle) override {
    auto agent =
        std::make_shared<SocketBasedUDFAgent>(pipe_handle, pipe_handle);

    std::shared_ptr<RequestHandler> handler =
        std::make_shared<StreamAggregateRequestHandler>(agent.get(), loop_);

    agent->setHandler(handler);
    return std::make_shared<AgentClient>(agent);
  }

 private:
  uvw::Loop* loop_;
};

class DynamicWindowUDFAgentClientFactory : public UnixSocketClientFactory {
 public:
  explicit DynamicWindowUDFAgentClientFactory(uvw::Loop* loop)
      : loop_(loop) {}

  std::shared_ptr<UnixSocketClient> createClient(
      const std::shared_ptr<uvw::PipeHandle>& pipe_handle) override {
    auto agent =
        std::make_shared<SocketBasedUDFAgent>(pipe_handle, pipe_handle);

    std::shared_ptr<RequestHandler> handler =
        std::make_shared<DynamicWindowRequestHandler>(agent.get(), loop_);

    agent->setHandler(handler);
    return std::make_shared<AgentClient>(agent);
  }

 private:
  uvw::Loop* loop_;
};

class ThresholdUDFAgentClientFactory : public UnixSocketClientFactory {
 public:
  std::shared_ptr<UnixSocketClient> createClient(
      const std::shared_ptr<uvw::PipeHandle>& pipe_handle) override {
    auto agent =
        std::make_shared<SocketBasedUDFAgent>(pipe_handle, pipe_handle);

    std::shared_ptr<RequestHandler> handler =
        std::make_shared<StatefulThresholdRequestHandler>(agent.get());

    agent->setHandler(handler);
    return std::make_shared<AgentClient>(agent);
  }
};

class DerivativeUDFAgentClientFactory : public UnixSocketClientFactory {
 public:
  explicit DerivativeUDFAgentClientFactory(uvw::Loop* loop) : loop_(loop) {}

  std::shared_ptr<UnixSocketClient> createClient(
      const std::shared_ptr<uvw::PipeHandle>& pipe_handle) override {
    auto agent =
        std::make_shared<SocketBasedUDFAgent>(pipe_handle, pipe_handle);

    std::shared_ptr<RequestHandler> handler =
        std::make_shared<DerivativeRequestHandler>(agent.get(), loop_);

    agent->setHandler(handler);
    return std::make_shared<AgentClient>(agent);
  }

 private:
  uvw::Loop* loop_;
};

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
