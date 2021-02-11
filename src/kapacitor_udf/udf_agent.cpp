#include <sstream>
#include <string>
#include <type_traits>

#include <spdlog/spdlog.h>

#include "udf_agent.h"
#include "utils/uvarint_utils.h"

namespace stream_data_processor {
namespace kapacitor_udf {

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
    : in_(std::move(in)), out_(std::move(out)) {
  in_->template on<uvw::DataEvent>(
      [this](
          const uvw::DataEvent& event,
          uvw::StreamHandle<UVWHandleType, LibuvHandleType>& /* unused */) {
        spdlog::debug("New data of size {}", event.length);
        std::istringstream data_stream(
            std::string(event.data.get(), event.length));
        if (!readLoop(data_stream)) {
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
  in_->shutdown();
  out_->shutdown();
  in_->stop();
  out_->stop();
  request_handler_->stop();
}

template void UDFAgent<uvw::TTYHandle, uv_tty_t>::stop();
template void UDFAgent<uvw::PipeHandle, uv_pipe_t>::stop();

template <typename UVWHandleType, typename LibuvHandleType>
void UDFAgent<UVWHandleType, LibuvHandleType>::writeResponse(
    const agent::Response& response) const {
  auto response_data = response.SerializeAsString();
  std::ostringstream out_stream;
  uvarint_utils::encode(out_stream, response_data.length());
  out_stream << response_data;
  spdlog::debug("Response: {}", response.DebugString());
  out_->write(out_stream.str().data(), out_stream.str().length());
}

template void UDFAgent<uvw::TTYHandle, uv_tty_t>::writeResponse(
    const agent::Response& response) const;
template void UDFAgent<uvw::PipeHandle, uv_pipe_t>::writeResponse(
    const agent::Response& response) const;

template <typename UVWHandleType, typename LibuvHandleType>
bool UDFAgent<UVWHandleType, LibuvHandleType>::readLoop(
    std::istream& input_stream) {
  agent::Request request;
  while (true) {
    try {
      uint32_t request_size = 0;
      if (residual_request_size_ == 0) {
        request_size = uvarint_utils::decode(input_stream);
      } else {
        request_size = residual_request_size_;
        residual_request_size_ = 0;
      }

      std::string request_data;
      request_data.resize(request_size);
      input_stream.read(request_data.data(), request_size);
      if (!residual_request_data_.empty()) {
        residual_request_data_.append(request_data);
        request_data = std::move(residual_request_data_);
        residual_request_data_.clear();
      }

      if (input_stream.gcount() < request_size) {
        residual_request_size_ = request_size - input_stream.gcount();
        residual_request_data_ =
            std::move(request_data.substr(0, input_stream.gcount()));
        return true;
      }

      request.ParseFromString(request_data);
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
    } catch (const uvarint_utils::EOFException&) {
      return true;
    } catch (const std::exception& exc) {
      reportError(fmt::format("error processing request: {}", exc.what()));
      return false;
    }
  }
}

template bool UDFAgent<uvw::TTYHandle, uv_tty_t>::readLoop(
    std::istream& input_stream);
template bool UDFAgent<uvw::PipeHandle, uv_pipe_t>::readLoop(
    std::istream& input_stream);

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

void AgentClient::start() { agent_->start(); }

void AgentClient::stop() { agent_->stop(); }

AgentClient::AgentClient(std::shared_ptr<IUDFAgent> agent)
    : agent_(std::move(agent)) {}

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
