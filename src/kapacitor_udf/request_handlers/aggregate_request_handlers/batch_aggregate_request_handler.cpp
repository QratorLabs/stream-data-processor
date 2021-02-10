#include <sstream>

#include "aggregate_options_parser.h"
#include "batch_aggregate_request_handler.h"
#include "kapacitor_udf/request_handlers/invalid_option_exception.h"

namespace stream_data_processor {
namespace kapacitor_udf {

const BasePointsConverter::PointsToRecordBatchesConversionOptions
    BatchAggregateRequestHandler::DEFAULT_TO_RECORD_BATCHES_OPTIONS{"time",
                                                                    "name"};

BatchAggregateRequestHandler::BatchAggregateRequestHandler(IUDFAgent* agent)
    : RecordBatchRequestHandler(agent, false) {
  setPointsConverter(std::make_shared<BasePointsConverter>(
      DEFAULT_TO_RECORD_BATCHES_OPTIONS));
}

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

  setHandler(
      std::make_shared<AggregateHandler>(std::move(aggregate_options)));
  response.mutable_init()->set_success(true);
  return response;
}

agent::Response BatchAggregateRequestHandler::snapshot() const {
  agent::Response response;
  std::stringstream snapshot_builder;
  if (in_batch_) {
    snapshot_builder.put('1');
  } else {
    snapshot_builder.put('0');
  }

  snapshot_builder << getPoints().SerializeAsString();
  response.mutable_snapshot()->set_snapshot(snapshot_builder.str());
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
  restorePointsFromSnapshotData(restore_request.snapshot().substr(1));
  response.mutable_restore()->set_success(true);
  return response;
}

void BatchAggregateRequestHandler::beginBatch(
    const agent::BeginBatch& batch) {
  in_batch_ = true;
}

void BatchAggregateRequestHandler::point(const agent::Point& point) {
  if (in_batch_) {
    addPoint(point);
  } else {
    agent::Response response;
    response.mutable_error()->set_error("Can't add point: not in batch");
    getAgent()->writeResponse(response);
  }
}

void BatchAggregateRequestHandler::endBatch(const agent::EndBatch& batch) {
  in_batch_ = false;
  setPointsName(batch.name());
  handleBatch();
}

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
