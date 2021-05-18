#include <sstream>
#include <string>
#include <type_traits>

#include <spdlog/spdlog.h>

#include "udf_agent.h"

namespace stream_data_processor {
namespace kapacitor_udf {

IUDFAgent::~IUDFAgent() = default;

template <typename UVWHandleType, typename LibuvHandleType>
UDFAgent<UVWHandleType, LibuvHandleType>::UDFAgent(uvw::Loop* /* unused */) {
  static_assert(std::is_same_v<UVWHandleType, uvw::TTYHandle> &&
                    std::is_same_v<LibuvHandleType, uv_tty_t>,
                "From-loop constructor is available for "
                "ChildProcessBasedUDFAgent only");
}

template <>
UDFAgent<uvw::TTYHandle, uv_tty_t>::UDFAgent(uvw::Loop* loop)
    : UDFAgent(loop->resource<uvw::TTYHandle>(uvw::StdIN, true),
               loop->resource<uvw::TTYHandle>(uvw::StdOUT, false)) {}

template <typename UVWHandleType, typename LibuvHandleType>
UDFAgent<UVWHandleType, LibuvHandleType>::UDFAgent(
    std::shared_ptr<uvw::StreamHandle<UVWHandleType, LibuvHandleType>> in,
    std::shared_ptr<uvw::StreamHandle<UVWHandleType, LibuvHandleType>> out)
    : in_(std::move(in)),
      out_(std::move(out)),
      kapacitor_request_reader_(
          std::make_unique<rw_utils::KapacitorRequestReader>()) {
  in_->template on<uvw::DataEvent>(
      [this](
          const uvw::DataEvent& event,
          uvw::StreamHandle<UVWHandleType, LibuvHandleType>& /* unused */) {
        spdlog::debug("New data of size {}", event.length);
        std::istringstream data_stream(
            std::string(event.data.get(), event.length));
        try {
          kapacitor_request_reader_->readRequests(data_stream,
                                                  handle_function_);
        } catch (const std::exception& exc) {
          reportError(
              fmt::format("error processing request: {}", exc.what()));
          stop();
        }
      });

  in_->template once<uvw::EndEvent>(
      [this](
          const uvw::EndEvent& event,
          uvw::StreamHandle<UVWHandleType, LibuvHandleType>& /* unused */) {
        spdlog::info("Connection closed");
        stop();
      });

  in_->template once<uvw::ErrorEvent>(
      [this](
          const uvw::ErrorEvent& event,
          uvw::StreamHandle<UVWHandleType, LibuvHandleType>& /* unused */) {
        spdlog::error(std::string(event.what()));
        stop();
      });
}

template UDFAgent<uvw::TTYHandle, uv_tty_t>::UDFAgent(
    std::shared_ptr<uvw::StreamHandle<uvw::TTYHandle, uv_tty_t>> in,
    std::shared_ptr<uvw::StreamHandle<uvw::TTYHandle, uv_tty_t>> out);
template UDFAgent<uvw::PipeHandle, uv_pipe_t>::UDFAgent(
    std::shared_ptr<uvw::StreamHandle<uvw::PipeHandle, uv_pipe_t>> in,
    std::shared_ptr<uvw::StreamHandle<uvw::PipeHandle, uv_pipe_t>> out);

template <typename UVWHandleType, typename LibuvHandleType>
void UDFAgent<UVWHandleType, LibuvHandleType>::setHandler(
    std::shared_ptr<RequestHandler> request_handler) {
  request_handler_ = std::move(request_handler);
}

template void UDFAgent<uvw::TTYHandle, uv_tty_t>::setHandler(
    std::shared_ptr<RequestHandler> request_handler);
template void UDFAgent<uvw::PipeHandle, uv_pipe_t>::setHandler(
    std::shared_ptr<RequestHandler> request_handler);

template <typename UVWHandleType, typename LibuvHandleType>
void UDFAgent<UVWHandleType, LibuvHandleType>::start() {
  request_handler_->start();
  in_->read();
}

template void UDFAgent<uvw::TTYHandle, uv_tty_t>::start();
template void UDFAgent<uvw::PipeHandle, uv_pipe_t>::start();

template <typename UVWHandleType, typename LibuvHandleType>
void UDFAgent<UVWHandleType, LibuvHandleType>::stop() {
  request_handler_->stop();
  in_->shutdown();
  out_->shutdown();
  in_->stop();
  out_->stop();
}

template void UDFAgent<uvw::TTYHandle, uv_tty_t>::stop();
template void UDFAgent<uvw::PipeHandle, uv_pipe_t>::stop();

template <typename UVWHandleType, typename LibuvHandleType>
void UDFAgent<UVWHandleType, LibuvHandleType>::writeResponse(
    const agent::Response& response) const {
  auto response_data = response.SerializeAsString();
  std::ostringstream out_stream;
  rw_utils::writeToStreamWithUvarintLength(out_stream, response_data);
  spdlog::debug("Response: {}", response.DebugString());
  auto data = out_stream.str();
  auto data_ptr = std::make_unique<char[]>(data.length() + 1);
  std::copy(data.begin(), data.end(), data_ptr.get());
  data_ptr[data.length()] = '\0';
  out_->write(std::move(data_ptr), data.length());
}

template void UDFAgent<uvw::TTYHandle, uv_tty_t>::writeResponse(
    const agent::Response& response) const;
template void UDFAgent<uvw::PipeHandle, uv_pipe_t>::writeResponse(
    const agent::Response& response) const;

template <typename UVWHandleType, typename LibuvHandleType>
void UDFAgent<UVWHandleType, LibuvHandleType>::reportError(
    const std::string& error_message) {
  spdlog::error(error_message);
  agent::Response response;
  response.mutable_error()->set_error(error_message);
  writeResponse(response);
}

template void UDFAgent<uvw::TTYHandle, uv_tty_t>::reportError(
    const std::string& error_message);
template void UDFAgent<uvw::PipeHandle, uv_pipe_t>::reportError(
    const std::string& error_message);

template <typename UVWHandleType, typename LibuvHandleType>
void UDFAgent<UVWHandleType, LibuvHandleType>::handleRequest(
    const agent::Request& request) const {
  spdlog::debug("Request: {}", request.DebugString());
  agent::Response response;
  switch (request.message_case()) {
    case agent::Request::kInfo:
      response = request_handler_->info();
      writeResponse(response);
      break;
    case agent::Request::kInit:
      response = request_handler_->init(request.init());
      writeResponse(response);
      break;
    case agent::Request::kKeepalive:
      response.mutable_keepalive()->set_time(request.keepalive().time());
      writeResponse(response);
      break;
    case agent::Request::kSnapshot:
      response = request_handler_->snapshot();
      writeResponse(response);
      break;
    case agent::Request::kRestore:
      response = request_handler_->restore(request.restore());
      writeResponse(response);
      break;
    case agent::Request::kBegin:
      request_handler_->beginBatch(request.begin());
      break;
    case agent::Request::kPoint:
      request_handler_->point(request.point());
      break;
    case agent::Request::kEnd:
      request_handler_->endBatch(request.end());
      break;
    default:
      spdlog::error("received unhandled request with enum number {}",
                    request.message_case());
  }
}

template void UDFAgent<uvw::TTYHandle, uv_tty_t>::handleRequest(
    const agent::Request& request) const;
template void UDFAgent<uvw::PipeHandle, uv_pipe_t>::handleRequest(
    const agent::Request& request) const;

void AgentClient::start() { agent_->start(); }

void AgentClient::stop() { agent_->stop(); }

AgentClient::AgentClient(std::shared_ptr<IUDFAgent> agent)
    : agent_(std::move(agent)) {}

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
