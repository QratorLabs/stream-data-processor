#include <spdlog/spdlog.h>

#include "kapacitor_udf/utils/grouping_utils.h"
#include "record_batch_request_handler.h"

namespace stream_data_processor {
namespace kapacitor_udf {

using convert_utils::BasePointsConverter;

RecordBatchRequestHandler::RecordBatchRequestHandler(
    const std::shared_ptr<IUDFAgent>& agent)
    : RequestHandler(agent) {}

void RecordBatchRequestHandler::handleBatch() {
  agent::Response response;

  if (handler_ == nullptr) {
    response.mutable_error()->set_error("RecordBatchHandler is not provided");
    getAgent()->writeResponse(response);
    return;
  }

  if (points_converter_ == nullptr) {
    response.mutable_error()->set_error("PointsConverter is not provided");
    getAgent()->writeResponse(response);
    return;
  }

  if (batch_points_.points_size() == 0) {
    return;
  }

  auto record_batches =
      points_converter_->convertToRecordBatches(batch_points_);

  batch_points_.mutable_points()->Clear();

  if (!record_batches.ok()) {
    response.mutable_error()->set_error(record_batches.status().message());
    getAgent()->writeResponse(response);
    return;
  }

  auto result = handler_->handle(record_batches.ValueOrDie());
  if (!result.ok()) {
    response.mutable_error()->set_error(result.status().message());
    getAgent()->writeResponse(response);
    return;
  }

  auto response_points_result =
      points_converter_->convertToPoints(result.ValueOrDie());

  if (!response_points_result.ok()) {
    response.mutable_error()->set_error(
        response_points_result.status().message());
    getAgent()->writeResponse(response);
    return;
  }

  auto response_points = response_points_result.ValueOrDie();

  for (auto& point : response_points.points()) {
    response.mutable_point()->CopyFrom(point);
    getAgent()->writeResponse(response);
  }
}

const agent::PointBatch& RecordBatchRequestHandler::getPoints() const {
  return batch_points_;
}

void RecordBatchRequestHandler::restorePointsFromSnapshotData(
    const std::string& data) {
  batch_points_.mutable_points()->Clear();
  batch_points_.ParseFromString(data);
}

void RecordBatchRequestHandler::addPoint(const agent::Point& point) {
  auto new_point = batch_points_.mutable_points()->Add();
  new_point->CopyFrom(point);
}

void RecordBatchRequestHandler::setPointsName(const std::string& name) {
  for (auto& point : *batch_points_.mutable_points()) {
    point.set_name(name);
  }
}

StreamRecordBatchRequestHandlerBase::StreamRecordBatchRequestHandlerBase(
    const std::shared_ptr<IUDFAgent>& agent)
    : RecordBatchRequestHandler(agent) {}

agent::Response StreamRecordBatchRequestHandlerBase::snapshot() const {
  agent::Response response;

  response.mutable_snapshot()->set_snapshot(getPoints().SerializeAsString());

  return response;
}

agent::Response StreamRecordBatchRequestHandlerBase::restore(
    const agent::RestoreRequest& restore_request) {
  agent::Response response;
  if (restore_request.snapshot().empty()) {
    response.mutable_restore()->set_success(false);
    response.mutable_restore()->set_error(
        "Can't restore from empty snapshot");
    return response;
  }

  restorePointsFromSnapshotData(restore_request.snapshot());
  response.mutable_restore()->set_success(true);
  return response;
}

void StreamRecordBatchRequestHandlerBase::beginBatch(
    const agent::BeginBatch& batch) {
  agent::Response response;
  response.mutable_error()->set_error(
      "Invalid BeginBatch request, UDF wants stream data");
  getAgent()->writeResponse(response);
}

void StreamRecordBatchRequestHandlerBase::endBatch(
    const agent::EndBatch& batch) {
  agent::Response response;
  response.mutable_error()->set_error(
      "Invalid EndBatch request, UDF wants stream data");
  getAgent()->writeResponse(response);
}

TimerRecordBatchRequestHandlerBase::TimerRecordBatchRequestHandlerBase(
    const std::shared_ptr<IUDFAgent>& agent, uvw::Loop* loop)
    : StreamRecordBatchRequestHandlerBase(agent),
      emit_timer_(loop->resource<uvw::TimerHandle>()) {
  emit_timer_->on<uvw::TimerEvent>(
      [this](const uvw::TimerEvent& /* non-used */,
             const uvw::TimerHandle& /* non-used */) { handleBatch(); });
}

TimerRecordBatchRequestHandlerBase::TimerRecordBatchRequestHandlerBase(
    const std::shared_ptr<IUDFAgent>& agent, uvw::Loop* loop,
    const std::chrono::seconds& batch_interval)
    : StreamRecordBatchRequestHandlerBase(agent),
      emit_timer_(loop->resource<uvw::TimerHandle>()),
      emit_timeout_(batch_interval) {
  emit_timer_->on<uvw::TimerEvent>(
      [this](const uvw::TimerEvent& /* non-used */,
             const uvw::TimerHandle& /* non-used */) { handleBatch(); });
}

void TimerRecordBatchRequestHandlerBase::point(const agent::Point& point) {
  addPoint(point);
  if (!emit_timer_->active()) {
    emit_timer_->start(emit_timeout_, emit_timeout_);
  }
}

void TimerRecordBatchRequestHandlerBase::stop() {
  handleBatch();
  emit_timer_->stop();
  RequestHandler::stop();
}

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
