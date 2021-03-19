#include "logging_request_handler.h"
#include "utils/uvarint_utils.h"

namespace stream_data_processor {
namespace kapacitor_udf {

stream_data_processor::kapacitor_udf::LoggingRequestHandler::
    LoggingRequestHandler(
        const stream_data_processor::kapacitor_udf::IUDFAgent* agent,
        std::ostream& logging_stream, bool enable_logging)
    : StreamRequestHandlerBase(agent),
      logging_stream_(logging_stream),
      is_logging_(enable_logging) {}

agent::Response LoggingRequestHandler::info() const {
  agent::Response response;
  response.mutable_info()->set_wants(agent::STREAM);
  response.mutable_info()->set_provides(agent::STREAM);
  return response;
}

agent::Response LoggingRequestHandler::init(
    const agent::InitRequest& init_request) {
  agent::Response response;
  response.mutable_init()->set_success(true);
  return response;
}

agent::Response LoggingRequestHandler::snapshot() const {
  agent::Response response;
  response.mutable_snapshot()->set_snapshot("");
  return response;
}

agent::Response LoggingRequestHandler::restore(
    const agent::RestoreRequest& restore_request) {
  agent::Response response;
  response.mutable_restore()->set_success(true);
  return response;
}

void LoggingRequestHandler::point(const agent::Point& point) {
  if (is_logging_) {
    auto serialized_point = point.SerializeAsString();
    uvarint_utils::encode(logging_stream_, serialized_point.size());
    logging_stream_ << serialized_point;
  }

  agent::Response mirror_point_response;
  *mirror_point_response.mutable_point() = point;
  getAgent()->writeResponse(mirror_point_response);
}

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
