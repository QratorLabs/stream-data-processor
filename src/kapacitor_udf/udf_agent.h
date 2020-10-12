#pragma once

#include <istream>
#include <memory>
#include <mutex>
#include <thread>

#include <uvw.hpp>

#include "request_handlers/request_handler.h"
#include "server/unix_socket_client.h"

#include "udf.pb.h"

class IUDFAgent {
 public:
  IUDFAgent() = default;

  IUDFAgent(const IUDFAgent&) = delete;
  IUDFAgent& operator=(const IUDFAgent&) = delete;

  virtual void start() = 0;
  virtual void stop() = 0;
  virtual void writeResponse(const agent::Response& response) = 0;

  virtual ~IUDFAgent() = default;
};

class RequestHandler;

template <typename UVWHandleType, typename LibuvHandleType>
class UDFAgent : public IUDFAgent {
 public:
  explicit UDFAgent(uvw::Loop* loop);
  UDFAgent(
      std::shared_ptr<uvw::StreamHandle<UVWHandleType, LibuvHandleType>> in,
      std::shared_ptr<uvw::StreamHandle<UVWHandleType, LibuvHandleType>> out);

  void setHandler(const std::shared_ptr<RequestHandler>& request_handler);

  void start() override;
  void stop() override;
  void writeResponse(const agent::Response& response) override;

 private:
  bool readLoop(std::istream& input_stream);
  void reportError(const std::string& error_message);

 private:
  std::shared_ptr<uvw::StreamHandle<UVWHandleType, LibuvHandleType>> in_;
  std::shared_ptr<uvw::StreamHandle<UVWHandleType, LibuvHandleType>> out_;
  std::shared_ptr<RequestHandler> request_handler_;
  std::string residual_request_data_;
  size_t residual_request_size_{0};
};

using ChildProcessBasedUDFAgent = UDFAgent<uvw::TTYHandle, uv_tty_t>;
using SocketBasedUDFAgent = UDFAgent<uvw::PipeHandle, uv_pipe_t>;

class AgentClient : public UnixSocketClient {
 public:
  explicit AgentClient(std::shared_ptr<IUDFAgent> agent);

  void start() override;
  void stop() override;

 private:
  std::shared_ptr<IUDFAgent> agent_;
};
