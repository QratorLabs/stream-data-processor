#include <sstream>

#include <spdlog/spdlog.h>

#include "batch_to_stream_request_handler.h"

#include "utils/serialize_utils.h"

namespace stream_data_processor {
namespace kapacitor_udf {

BatchToStreamRequestHandler::BatchToStreamRequestHandler(
    const std::shared_ptr<IUDFAgent>& agent)
    : RecordBatchRequestHandler(agent) {}

agent::Response BatchToStreamRequestHandler::info() const {
  agent::Response response;
  response.mutable_info()->set_wants(agent::EdgeType::BATCH);
  response.mutable_info()->set_provides(agent::EdgeType::STREAM);
  return response;
}

agent::Response BatchToStreamRequestHandler::init(
    const agent::InitRequest& init_request) {
  agent::Response response;
  response.mutable_init()->set_success(true);
  return response;
}

agent::Response BatchToStreamRequestHandler::snapshot() const {
  agent::Response response;
  std::stringstream snapshot_builder;
  if (in_batch_) {
    snapshot_builder << '1';
  } else {
    snapshot_builder << '0';
  }

  snapshot_builder << getPoints().SerializeAsString();
  response.mutable_snapshot()->set_snapshot(snapshot_builder.str());
  return response;
}

agent::Response BatchToStreamRequestHandler::restore(
    const agent::RestoreRequest& restore_request) {
  agent::Response response;
  if (restore_request.snapshot().empty()) {
    response.mutable_restore()->set_success(false);
    response.mutable_restore()->set_error(
        "Can't restore from empty snapshot");
    return response;
  }

  if (restore_request.snapshot()[0] != '0' &&
      restore_request.snapshot()[0] != '1') {
    response.mutable_restore()->set_success(false);
    response.mutable_restore()->set_error("Invalid snapshot");
    return response;
  }

  in_batch_ = restore_request.snapshot()[0] == '1';
  restorePointsFromSnapshotData(restore_request.snapshot().substr(1));
  response.mutable_restore()->set_success(true);
  return response;
}

void BatchToStreamRequestHandler::beginBatch(const agent::BeginBatch& batch) {
  in_batch_ = true;
}

void BatchToStreamRequestHandler::point(const agent::Point& point) {
  if (in_batch_) {
    addPoint(point);
  } else {
    agent::Response response;
    response.mutable_error()->set_error("Can't add point: not in batch");
    getAgent()->writeResponse(response);
  }
}

void BatchToStreamRequestHandler::endBatch(const agent::EndBatch& batch) {
  in_batch_ = false;
  handleBatch();
}

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
