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

agent::Response BatchToStreamRequestHandler::snapshot() {         // TODO: make snapshot using protobufs only
  agent::Response response;
  arrow::RecordBatchVector record_batches;
  auto convert_result = DataConverter::convertToRecordBatches(batch_points_, record_batches, to_record_batches_options_);
  if (!convert_result.ok()) {
    response.mutable_error()->set_error(convert_result.message());
    return response;
  }

  if (record_batches.empty()) {
    response.mutable_snapshot()->set_snapshot(in_batch_ ? "1" : "0");
    return response;
  }

  std::shared_ptr<arrow::Buffer> buffer;
  auto serialize_result = Serializer::serializeRecordBatches(record_batches.back()->schema(), record_batches, &buffer);
  if (!serialize_result.ok()) {
    response.mutable_error()->set_error(serialize_result.message());
    return response;
  }

  response.mutable_snapshot()->set_snapshot((in_batch_ ? "1" : "0") + buffer->ToString());
  return response;
}

agent::Response BatchToStreamRequestHandler::restore(const agent::RestoreRequest &restore_request) {    // TODO: make snapshot using protobufs only
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

  if (restore_request.snapshot().size() == 1) {
    batch_points_.clear();
    response.mutable_restore()->set_success(true);
    return response;
  }

  auto buffer = arrow::Buffer::FromString(restore_request.snapshot().substr(1));
  arrow::RecordBatchVector record_batches;
  auto deserialize_result = Serializer::deserializeRecordBatches(buffer, &record_batches);
  if (!deserialize_result.ok()) {
    response.mutable_restore()->set_success(false);
    response.mutable_restore()->set_error(deserialize_result.message());
    return response;
  }

  batch_points_.clear();
  auto convert_result = DataConverter::convertToPoints(record_batches, batch_points_, to_points_options_);
  if (!convert_result.ok()) {
    response.mutable_restore()->set_success(false);
    response.mutable_restore()->set_error(convert_result.message());
    return response;
  }

  response.mutable_restore()->set_success(true);
  return response;
}

void BatchToStreamRequestHandler::beginBatch(const agent::BeginBatch &batch) {
  in_batch_ = true;
}

void BatchToStreamRequestHandler::point(const agent::Point &point) {
  if (in_batch_) {
    batch_points_.push_back(point);
  } else {
    agent::Response response;
    response.mutable_error()->set_error("Can't add point: not in batch");
    agent_.writeResponse(response);
  }
}

void BatchToStreamRequestHandler::endBatch(const agent::EndBatch &batch) {
  agent::Response response;
  arrow::RecordBatchVector record_batches;
  auto convert_result = DataConverter::convertToRecordBatches(batch_points_, record_batches, to_record_batches_options_);
  batch_points_.clear();
  in_batch_ = false;
  if (!convert_result.ok()) {
    response.mutable_error()->set_error(convert_result.message());
    agent_.writeResponse(response);
    return;
  }

  for (auto& handler : handlers_pipeline_) {
    arrow::RecordBatchVector result;
    auto handle_result = handler->handle(record_batches, result);
    if (!handle_result.ok()) {
      response.mutable_error()->set_error(handle_result.message());
      agent_.writeResponse(response);
      return;
    }

    record_batches = std::move(result);
  }

  std::vector<agent::Point> response_points;
  convert_result = DataConverter::convertToPoints(record_batches, response_points, to_points_options_);
  if (!convert_result.ok()) {
    response.mutable_error()->set_error(convert_result.message());
    agent_.writeResponse(response);
    return;
  }

  for (auto& point : response_points) {
    response.mutable_point()->CopyFrom(point);
    agent_.writeResponse(response);
  }
}
