#include "spdlog/spdlog.h"

#include "agent.h"
#include "utils/uvarint.h"

Agent::Agent(std::istream &in, std::ostream &out) : in_(in), out_(out) {

}

void Agent::setHandler(const std::shared_ptr<RequestHandler> &request_handler) {
  request_handler_ = request_handler;
}

void Agent::start() {
  read_loop_thread_ = std::move(std::thread([this]() {
    readLoop();
  }));
}

void Agent::wait() {
  read_loop_thread_.join();
}

void Agent::writeResponse(const agent::Response &response) {
  std::lock_guard write_lock(write_mutex_);
  auto response_data = response.SerializeAsString();
  UVarIntCoder::encode(out_, response_data.length());
  out_ << response_data;
  out_.flush();
}

void Agent::readLoop() {
  agent::Request request;
  while (true) {
    try {
      auto request_size = UVarIntCoder::decode(in_);
      std::string request_data;
      request_data.resize(request_size);
      in_.read(request_data.data(), request_size);
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
      break;
    } catch (const std::exception& exc) {
      std::string error_message("error processing request with enum number " + std::to_string(request.message_case()) +
                                ": " + exc.what());
      spdlog::error(error_message);
      agent::Response response;
      response.mutable_error()->set_error(error_message);
      writeResponse(response);
      break;
    }
  }
}
