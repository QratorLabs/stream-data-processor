#include <spdlog/spdlog.h>

#include "batch_request_handler.h"

namespace stream_data_processor {
namespace kapacitor_udf {

BatchRequestHandler::BatchRequestHandler(
    const IUDFAgent* agent, std::unique_ptr<IPointsStorage>&& points_storage)
    : BatchRecordBatchRequestHandlerBase(agent) {
  setPointsStorage(std::move(points_storage));
}

agent::Response BatchRequestHandler::info() const {
  agent::Response response;
  response.mutable_info()->set_wants(agent::EdgeType::BATCH);
  if (getPointsStorage()->providesBatch()) {
    response.mutable_info()->set_provides(agent::EdgeType::BATCH);
  } else {
    response.mutable_info()->set_provides(agent::EdgeType::STREAM);
  }

  return response;
}

agent::Response BatchRequestHandler::init(
    const agent::InitRequest& init_request) {
  agent::Response response;
  response.mutable_init()->set_success(true);
  return response;
}

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
