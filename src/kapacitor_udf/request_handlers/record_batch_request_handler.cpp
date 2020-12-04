#include <spdlog/spdlog.h>

#include "kapacitor_udf/utils/grouping_utils.h"
#include "record_batch_request_handler.h"

namespace stream_data_processor {
namespace kapacitor_udf {

using convert_utils::PointsConverter;

RecordBatchRequestHandler::RecordBatchRequestHandler(
    const std::shared_ptr<IUDFAgent>& agent,
    PointsConverter::PointsToRecordBatchesConversionOptions
        to_record_batches_options,
    const std::shared_ptr<RecordBatchHandler>& handler)
    : RequestHandler(agent),
      handler_(handler),
      to_record_batches_options_(std::move(to_record_batches_options)) {}

void RecordBatchRequestHandler::handleBatch() {
  agent::Response response;

  if (handler_ == nullptr) {
    response.mutable_error()->set_error("RecordBatchHandler is not provided");
    getAgent()->writeResponse(response);
    return;
  }

  auto record_batches = PointsConverter::convertToRecordBatches(
      batch_points_, to_record_batches_options_);

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

  auto response_points =
      PointsConverter::convertToPoints(result.ValueOrDie());

  if (!response_points.ok()) {
    response.mutable_error()->set_error(response_points.status().message());
    getAgent()->writeResponse(response);
    return;
  }

  for (auto& point : response_points.ValueOrDie().points()) {
    response.mutable_point()->CopyFrom(point);
    getAgent()->writeResponse(response);
  }
}

std::shared_ptr<RecordBatchHandler> RecordBatchRequestHandler::getHandler()
    const {
  return handler_;
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
    const std::shared_ptr<IUDFAgent>& agent,
    PointsConverter::PointsToRecordBatchesConversionOptions
        to_record_batches_options,
    const std::shared_ptr<RecordBatchHandler>& handler)
    : RecordBatchRequestHandler(agent, to_record_batches_options, handler) {}

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

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
