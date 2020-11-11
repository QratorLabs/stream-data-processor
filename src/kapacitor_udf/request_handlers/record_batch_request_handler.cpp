#include <spdlog/spdlog.h>

#include "kapacitor_udf/utils/grouping_utils.h"
#include "record_batch_request_handler.h"

#include "utils/convert_utils.h"

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
    agent_.lock()->writeResponse(response);
    return;
  }

  arrow::RecordBatchVector record_batches;
  auto convert_result = PointsConverter::convertToRecordBatches(
      batch_points_, &record_batches, to_record_batches_options_);

  batch_points_.mutable_points()->Clear();

  if (!convert_result.ok()) {
    response.mutable_error()->set_error(convert_result.message());
    agent_.lock()->writeResponse(response);
    return;
  }

  arrow::RecordBatchVector result;
  auto handle_result = handler_->handle(record_batches, &result);
  if (!handle_result.ok()) {
    response.mutable_error()->set_error(handle_result.message());
    agent_.lock()->writeResponse(response);
    return;
  }

  record_batches = std::move(result);

  agent::PointBatch response_points;
  convert_result =
      PointsConverter::convertToPoints(record_batches, &response_points);

  if (!convert_result.ok()) {
    response.mutable_error()->set_error(convert_result.message());
    agent_.lock()->writeResponse(response);
    return;
  }

  for (auto& point : response_points.points()) {
    response.mutable_point()->CopyFrom(point);
    agent_.lock()->writeResponse(response);
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

  response.mutable_snapshot()->set_snapshot(
      batch_points_.SerializeAsString());

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

  batch_points_.mutable_points()->Clear();
  batch_points_.ParseFromString(restore_request.snapshot());
  response.mutable_restore()->set_success(true);
  return response;
}

void StreamRecordBatchRequestHandlerBase::beginBatch(
    const agent::BeginBatch& batch) {
  agent::Response response;
  response.mutable_error()->set_error(
      "Invalid BeginBatch request, UDF wants stream data");
  agent_.lock()->writeResponse(response);
}

void StreamRecordBatchRequestHandlerBase::endBatch(
    const agent::EndBatch& batch) {
  agent::Response response;
  response.mutable_error()->set_error(
      "Invalid EndBatch request, UDF wants stream data");
  agent_.lock()->writeResponse(response);
}

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
