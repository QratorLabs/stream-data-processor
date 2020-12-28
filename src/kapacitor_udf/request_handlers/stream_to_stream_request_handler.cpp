#include "stream_to_stream_request_handler.h"

namespace stream_data_processor {
namespace kapacitor_udf {

StreamToStreamRequestHandler::StreamToStreamRequestHandler(
    const std::shared_ptr<IUDFAgent>& agent, uvw::Loop* loop,
    const std::chrono::duration<uint64_t>& batch_interval)
    : TimerRecordBatchRequestHandlerBase(agent, loop, batch_interval) {}

agent::Response StreamToStreamRequestHandler::info() const {
  agent::Response response;
  response.mutable_info()->set_wants(agent::EdgeType::STREAM);
  response.mutable_info()->set_provides(agent::EdgeType::STREAM);
  return response;
}

agent::Response StreamToStreamRequestHandler::init(
    const agent::InitRequest& init_request) {
  agent::Response response;
  response.mutable_init()->set_success(true);
  return response;
}

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
