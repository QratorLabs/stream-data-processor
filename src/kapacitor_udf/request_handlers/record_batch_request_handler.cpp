#include <spdlog/spdlog.h>

#include "record_batch_request_handler.h"
#include "kapacitor_udf/group_parser.h"

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
  convert_result = PointsConverter::convertToPoints(
      record_batches, &response_points);

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
