#include "stream_to_stream_request_handler.h"

agent::Response StreamToStreamRequestHandler::info() const {
  agent::Response response;
  response.mutable_info()->set_wants(agent::EdgeType::STREAM);
  response.mutable_info()->set_provides(agent::EdgeType::STREAM);
  return response;
}

agent::Response StreamToStreamRequestHandler::init(const agent::InitRequest &init_request) {
  agent::Response response;
  response.mutable_init()->set_success(true);
  return response;
}

agent::Response StreamToStreamRequestHandler::snapshot() const {
  agent::Response response;
  response.mutable_snapshot()->set_snapshot(batch_points_.SerializeAsString());
  return response;
}

agent::Response StreamToStreamRequestHandler::restore(const agent::RestoreRequest &restore_request) {
  agent::Response response;
  batch_points_.mutable_points()->Clear();
  batch_points_.ParseFromString(restore_request.snapshot());
  response.mutable_restore()->set_success(true);
  return response;
}

void StreamToStreamRequestHandler::beginBatch(const agent::BeginBatch &batch) {
  agent::Response response;
  response.mutable_error()->set_error("Invalid BeginBatch request, UDF wants stream data");
  agent_.lock()->writeResponse(response);
}

void StreamToStreamRequestHandler::point(const agent::Point &point) {
  auto new_point = batch_points_.mutable_points()->Add();
  new_point->CopyFrom(point);
  if (!batch_timer_->active()) {
    batch_timer_->start(batch_interval_, batch_interval_);
  }
}

void StreamToStreamRequestHandler::endBatch(const agent::EndBatch &batch) {
  agent::Response response;
  response.mutable_error()->set_error("Invalid EndBatch request, UDF wants stream data");
  agent_.lock()->writeResponse(response);
}

void StreamToStreamRequestHandler::stop() {
  handleBatch();
  batch_timer_->stop();
}
