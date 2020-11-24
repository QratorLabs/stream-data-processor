#include "batch_aggregate_request_handler.h"
#include "aggregate_options_parser.h"
#include "kapacitor_udf/request_handlers/invalid_option_exception.h"
#include "record_batch_handlers/record_batch_handlers.h"

namespace stream_data_processor {
namespace kapacitor_udf {

const PointsConverter::PointsToRecordBatchesConversionOptions
    BatchAggregateRequestHandler::DEFAULT_TO_RECORD_BATCHES_OPTIONS{"time",
                                                                    "name"};

BatchAggregateRequestHandler::BatchAggregateRequestHandler(
    const std::shared_ptr<IUDFAgent>& agent)
    : RecordBatchRequestHandler(agent, DEFAULT_TO_RECORD_BATCHES_OPTIONS) {}

agent::Response BatchAggregateRequestHandler::info() const {
  agent::Response response;
  response.mutable_info()->set_wants(agent::EdgeType::BATCH);
  response.mutable_info()->set_provides(agent::EdgeType::STREAM);
  *response.mutable_info()->mutable_options() =
      AggregateOptionsParser::getResponseOptionsMap();
  return response;
}

agent::Response BatchAggregateRequestHandler::init(
    const agent::InitRequest& init_request) {
  agent::Response response;
  AggregateHandler::AggregateOptions aggregate_options;

  try {
    aggregate_options =
        AggregateOptionsParser::parseOptions(init_request.options());
  } catch (const InvalidOptionException& exc) {
    response.mutable_init()->set_success(false);
    response.mutable_init()->set_error(exc.what());
    return response;
  }

  auto aggregate_handler =
      std::make_shared<AggregateHandler>(std::move(aggregate_options));
  handler_ = std::make_shared<PipelineHandler>();
  std::static_pointer_cast<PipelineHandler>(handler_)->pushBackHandler(
      std::move(aggregate_handler));

  response.mutable_init()->set_success(true);
  return response;
}

agent::Response BatchAggregateRequestHandler::snapshot() const {
  agent::Response response;
  response.mutable_snapshot()->set_snapshot(
      (in_batch_ ? "1" : "0") + batch_points_.SerializeAsString());
  return response;
}

agent::Response BatchAggregateRequestHandler::restore(
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
  batch_points_.mutable_points()->Clear();
  batch_points_.ParseFromString(restore_request.snapshot().substr(1));
  response.mutable_restore()->set_success(true);
  return response;
}

void BatchAggregateRequestHandler::beginBatch(
    const agent::BeginBatch& batch) {
  in_batch_ = true;
}

void BatchAggregateRequestHandler::point(const agent::Point& point) {
  if (in_batch_) {
    auto new_point = batch_points_.mutable_points()->Add();
    new_point->CopyFrom(point);
  } else {
    agent::Response response;
    response.mutable_error()->set_error("Can't add point: not in batch");
    agent_.lock()->writeResponse(response);
  }
}

void BatchAggregateRequestHandler::endBatch(const agent::EndBatch& batch) {
  in_batch_ = false;
  for (auto& point : *batch_points_.mutable_points()) {
    point.set_name(batch.name());
  }

  handleBatch();
}

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
