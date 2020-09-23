#include "pipeline_request_handler.h"

void PipelineRequestHandler::handleBatch() {
  agent::Response response;
  arrow::RecordBatchVector record_batches;
  auto convert_result = DataConverter::convertToRecordBatches(batch_points_, record_batches, to_record_batches_options_);
  batch_points_.mutable_points()->Clear();
  if (!convert_result.ok()) {
    response.mutable_error()->set_error(convert_result.message());
    agent_.lock()->writeResponse(response);
    return;
  }

  for (auto& handler : handlers_pipeline_) {
    arrow::RecordBatchVector result;
    auto handle_result = handler->handle(record_batches, result);
    if (!handle_result.ok()) {
      response.mutable_error()->set_error(handle_result.message());
      agent_.lock()->writeResponse(response);
      return;
    }

    record_batches = std::move(result);
  }

  agent::PointBatch response_points;
  convert_result = DataConverter::convertToPoints(record_batches, response_points, to_points_options_);
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
