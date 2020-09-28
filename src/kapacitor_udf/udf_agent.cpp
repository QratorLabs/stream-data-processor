#include <sstream>
#include <string>

#include <spdlog/spdlog.h>

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
  in_->template on<uvw::DataEvent>([this](const uvw::DataEvent& event, uvw::StreamHandle<UVWHandleType, LibuvHandleType>) {
    std::stringstream data_stream(std::string(event.data.get(), event.length));
    if (!readLoop(data_stream)) {
      stop();
    }
  });

  in_->template on<uvw::ErrorEvent>([this](const uvw::ErrorEvent& event, uvw::StreamHandle<UVWHandleType, LibuvHandleType>) {
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
  in_->stop();
  out_->stop();
}

template<typename UVWHandleType, typename LibuvHandleType>
void UDFAgent<UVWHandleType, LibuvHandleType>::writeResponse(const agent::Response &response) {
  auto response_data = response.SerializeAsString();
  std::stringstream out_stream;
  UVarIntCoder::encode(out_stream, response_data.length());
  out_stream << response_data;
  out_->write(out_stream.str().data(), out_stream.str().length());
}

template<typename UVWHandleType, typename LibuvHandleType>
bool UDFAgent<UVWHandleType, LibuvHandleType>::readLoop(std::istream& input_stream) {
  agent::Request request;
  while (true) {
    try {
      uint32_t request_size;
      if (residual_request_size_ == 0) {
        request_size = UVarIntCoder::decode(input_stream);
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
        request_data.clear();
      }

      if (input_stream.gcount() < request_size) {
        residual_request_size_ = request_size - input_stream.gcount();
        residual_request_data_ = std::move(request_data.substr(0, input_stream.gcount()));
        return true;
      }

      request.ParseFromString(request_data);
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
