#include <sstream>
#include <string>

#include <spdlog/spdlog.h>
#include <spdlog/fmt/bin_to_hex.h>


#include "udf_agent.h"
#include "utils/uvarint.h"

template<>
UDFAgent<uvw::TTYHandle, uv_tty_t>::UDFAgent(const std::shared_ptr<uvw::Loop> &loop)
    : in_(loop->resource<uvw::TTYHandle>(uvw::StdIN, true))
    , out_(loop->resource<uvw::TTYHandle>(uvw::StdOUT, false)) {

}

template<typename UVWHandleType, typename LibuvHandleType>
UDFAgent<UVWHandleType, LibuvHandleType>::UDFAgent(std::shared_ptr<uvw::StreamHandle<UVWHandleType, LibuvHandleType>> in,
                                                   std::shared_ptr<uvw::StreamHandle<UVWHandleType, LibuvHandleType>> out)
    : in_(std::move(in))
    , out_(std::move(out)) {
  in_->template on<uvw::DataEvent>([this](const uvw::DataEvent& event, uvw::StreamHandle<UVWHandleType, LibuvHandleType>& handle) {
    spdlog::debug("New data of size {}: {}", event.length, spdlog::to_hex(std::string(event.data.get(), event.length)));
    std::istringstream data_stream(std::string(event.data.get(), event.length));
    if (!readLoop(data_stream)) {
      stop();
    }
  });

  out_->template on<uvw::WriteEvent>([](const uvw::WriteEvent& event, uvw::StreamHandle<UVWHandleType, LibuvHandleType>& handle) {
    spdlog::debug("Data has been written");
  });

  in_->template once<uvw::EndEvent>([this](const uvw::EndEvent& event, uvw::StreamHandle<UVWHandleType, LibuvHandleType>& handle) {
    spdlog::info("Connection closed");
    stop();
  });

  in_->template once<uvw::ErrorEvent>([this](const uvw::ErrorEvent& event, uvw::StreamHandle<UVWHandleType, LibuvHandleType>& handle) {
    reportError(std::string(event.what()));
    stop();
  });
}

template<typename UVWHandleType, typename LibuvHandleType>
void UDFAgent<UVWHandleType, LibuvHandleType>::setHandler(const std::shared_ptr<RequestHandler> &request_handler) {
  request_handler_ = request_handler;
}

template<typename UVWHandleType, typename LibuvHandleType>
void UDFAgent<UVWHandleType, LibuvHandleType>::start() {
  in_->read();
}

template<typename UVWHandleType, typename LibuvHandleType>
void UDFAgent<UVWHandleType, LibuvHandleType>::stop() {
  in_->shutdown();
  out_->shutdown();
  in_->stop();
  out_->stop();
}

template<typename UVWHandleType, typename LibuvHandleType>
void UDFAgent<UVWHandleType, LibuvHandleType>::writeResponse(const agent::Response &response) {
  auto response_data = response.SerializeAsString();
  std::ostringstream out_stream;
  UVarIntCoder::encode(out_stream, response_data.length());
  out_stream << response_data;
  spdlog::debug("Response: {}", response.DebugString());
  spdlog::debug("Handle is writable: {}", out_->writable());
  out_->write(out_stream.str().data(), out_stream.str().length());
  spdlog::debug("Response sent");
}

template<typename UVWHandleType, typename LibuvHandleType>
bool UDFAgent<UVWHandleType, LibuvHandleType>::readLoop(std::istream& input_stream) {
  agent::Request request;
  while (true) {
    try {
      spdlog::debug("Residual size: {}, residual string: {}", residual_request_size_, spdlog::to_hex(residual_request_data_));
      uint32_t request_size;
      if (residual_request_size_ == 0) {
        request_size = UVarIntCoder::decode(input_stream);
      } else {
        request_size = residual_request_size_;
        residual_request_size_ = 0;
      }

      spdlog::debug("Decoded request size: {}", request_size);
      std::string request_data;
      request_data.resize(request_size);
      input_stream.read(request_data.data(), request_size);
      if (!residual_request_data_.empty()) {
        residual_request_data_.append(request_data);
        request_data = std::move(residual_request_data_);
        residual_request_data_.clear();
      }

      spdlog::debug("Read {} bytes", input_stream.gcount());
      if (input_stream.gcount() < request_size) {
        residual_request_size_ = request_size - input_stream.gcount();
        residual_request_data_ = std::move(request_data.substr(0, input_stream.gcount()));
        return true;
      }

      request.ParseFromString(request_data);
      spdlog::debug("Request: {}", request.DebugString());
      agent::Response response;
      switch (request.message_case()) {
        case agent::Request::kInfo:
          response = std::move(request_handler_->info());
          writeResponse(response);
          break;
        case agent::Request::kInit:
          response = std::move(request_handler_->init(request.init()));
          writeResponse(response);
          break;
        case agent::Request::kKeepalive:
          response.mutable_keepalive()->set_time(request.keepalive().time());
          writeResponse(response);
          break;
        case agent::Request::kSnapshot:
          response = std::move(request_handler_->snapshot());
          writeResponse(response);
          break;
        case agent::Request::kRestore:
          response = std::move(request_handler_->restore(request.restore()));
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
          spdlog::error("received unhandled request with enum number {}", request.message_case());
      }
    } catch (const EOFException&) {
      spdlog::debug("EOF reached");
      return true;
    } catch (const std::exception& exc) {
      std::string error_message = fmt::format("error processing request with enum number {}: {}",
                                              std::to_string(request.message_case()),
                                              exc.what());
      reportError(error_message);
      return false;
    }
  }
}

template<typename UVWHandleType, typename LibuvHandleType>
void UDFAgent<UVWHandleType, LibuvHandleType>::reportError(const std::string& error_message) {
  spdlog::error(error_message);
  agent::Response response;
  response.mutable_error()->set_error(error_message);
  writeResponse(response);
}

template class UDFAgent<uvw::TTYHandle, uv_tty_t>;
template class UDFAgent<uvw::PipeHandle, uv_pipe_t>;
