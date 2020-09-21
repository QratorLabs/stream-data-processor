#include "batch_to_stream_request_handler.h"

#include "utils/data_converter.h"
#include "utils/serializer.h"

agent::Response BatchToStreamRequestHandler::info() const {
  agent::Response response;
  response.mutable_info()->set_wants(agent::EdgeType::BATCH);
  response.mutable_info()->set_provides(agent::EdgeType::STREAM);
  return response;
}

agent::Response BatchToStreamRequestHandler::init(const agent::InitRequest &init_request) {
  agent::Response response;
  response.mutable_init()->set_success(true);
  return response;
}

agent::Response BatchToStreamRequestHandler::snapshot() {
  agent::Response response;
  response.mutable_snapshot()->set_snapshot((in_batch_ ? "1" : "0") + batch_points_.SerializeAsString());
  return response;
}

agent::Response BatchToStreamRequestHandler::restore(const agent::RestoreRequest &restore_request) {
  agent::Response response;
  if (restore_request.snapshot().empty()) {
    response.mutable_restore()->set_success(false);
    response.mutable_restore()->set_error("Can't restore from empty snapshot");
    return response;
  }

  if (restore_request.snapshot()[0] != '0' && restore_request.snapshot()[0] != '1') {
    response.mutable_restore()->set_success(false);
    response.mutable_restore()->set_error("Invalid snapshot");
    return response;
  }

  in_batch_ = restore_request.snapshot()[0] == '1';
  batch_points_.mutable_points()->Clear();
  batch_points_.ParseFromString(restore_request.snapshot().substr(1));
  response.mutable_restore()->set_success(true);
  return response;
}

void BatchToStreamRequestHandler::beginBatch(const agent::BeginBatch &batch) {
  in_batch_ = true;
}

void BatchToStreamRequestHandler::point(const agent::Point &point) {
  if (in_batch_) {
    agent::Point copy_point;
    copy_point.CopyFrom(point);
    batch_points_.mutable_points()->Add(std::move(copy_point));
  } else {
    agent::Response response;
    response.mutable_error()->set_error("Can't add point: not in batch");
    agent_.lock()->writeResponse(response);
  }
}

void BatchToStreamRequestHandler::endBatch(const agent::EndBatch &batch) {
  agent::Response response;
  arrow::RecordBatchVector record_batches;
  auto convert_result = DataConverter::convertToRecordBatches(batch_points_, record_batches, to_record_batches_options_);
  batch_points_.mutable_points()->Clear();
  in_batch_ = false;
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
