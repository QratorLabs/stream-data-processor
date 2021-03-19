#pragma once

#include <functional>
#include <istream>
#include <memory>
#include <mutex>
#include <thread>

#include <uvw.hpp>

#include "request_handlers/request_handler.h"
#include "server/unix_socket_client.h"
#include "utils/rw_utils.h"

#include "udf.pb.h"

namespace stream_data_processor {
namespace kapacitor_udf {

class IUDFAgent {
 public:
  virtual void start() = 0;
  virtual void stop() = 0;
  virtual void writeResponse(const agent::Response& response) const = 0;

  virtual ~IUDFAgent() = 0;

 protected:
  IUDFAgent() = default;

  IUDFAgent(const IUDFAgent& /* non-used */) = default;
  IUDFAgent& operator=(const IUDFAgent& /* non-used */) = default;

  IUDFAgent(IUDFAgent&& /* non-used */) = default;
  IUDFAgent& operator=(IUDFAgent&& /* non-used */) = default;
};

class RequestHandler;

template <typename UVWHandleType, typename LibuvHandleType>
class UDFAgent : public IUDFAgent {
 public:
  explicit UDFAgent(uvw::Loop* loop);
  UDFAgent(
      std::shared_ptr<uvw::StreamHandle<UVWHandleType, LibuvHandleType>> in,
      std::shared_ptr<uvw::StreamHandle<UVWHandleType, LibuvHandleType>> out);

  void setHandler(std::shared_ptr<RequestHandler> request_handler);

  void start() override;
  void stop() override;
  void writeResponse(const agent::Response& response) const override;

 private:
  void reportError(const std::string& error_message);
  void handleRequest(const agent::Request& request) const;

 private:
  const std::function<void(const agent::Request&)> handle_function_{
      [this](const agent::Request& request) { handleRequest(request); }};

  std::shared_ptr<uvw::StreamHandle<UVWHandleType, LibuvHandleType>> in_;
  std::shared_ptr<uvw::StreamHandle<UVWHandleType, LibuvHandleType>> out_;
  std::unique_ptr<rw_utils::IKapacitorRequestReader>
      kapacitor_request_reader_;
  std::shared_ptr<RequestHandler> request_handler_;
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

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
