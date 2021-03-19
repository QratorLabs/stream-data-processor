#include "stream_request_handler.h"

namespace stream_data_processor {
namespace kapacitor_udf {

StreamRequestHandler::StreamRequestHandler(
    const IUDFAgent* agent, uvw::Loop* loop,
    std::chrono::duration<uint64_t> batch_interval,
    std::unique_ptr<IPointsStorage>&& points_storage)
    : TimerRecordBatchRequestHandlerBase(agent, loop, batch_interval) {
  setPointsStorage(std::move(points_storage));
}

agent::Response StreamRequestHandler::info() const {
  agent::Response response;
  response.mutable_info()->set_wants(agent::EdgeType::STREAM);
  if (getPointsStorage()->providesBatch()) {
    response.mutable_info()->set_provides(agent::EdgeType::BATCH);
  } else {
    response.mutable_info()->set_provides(agent::EdgeType::STREAM);
  }

  return response;
}

agent::Response StreamRequestHandler::init(
    const agent::InitRequest& init_request) {
  agent::Response response;
  response.mutable_init()->set_success(true);
  return response;
}

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
