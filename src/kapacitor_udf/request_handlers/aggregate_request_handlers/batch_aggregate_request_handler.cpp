#include <sstream>

#include "aggregate_options_parser.h"
#include "batch_aggregate_request_handler.h"
#include "kapacitor_udf/request_handlers/invalid_option_exception.h"

namespace stream_data_processor {
namespace kapacitor_udf {

const BasePointsConverter::PointsToRecordBatchesConversionOptions
    BatchAggregateRequestHandler::DEFAULT_TO_RECORD_BATCHES_OPTIONS{"time",
                                                                    "name"};

BatchAggregateRequestHandler::BatchAggregateRequestHandler(
    const IUDFAgent* agent)
    : BatchRecordBatchRequestHandlerBase(agent) {}

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

  std::unique_ptr<RecordBatchHandler> handler =
      std::make_unique<AggregateHandler>(std::move(aggregate_options));

  setPointsStorage(std::make_unique<storage_utils::PointsStorage>(
      getAgent(),
      std::make_unique<BasePointsConverter>(
          DEFAULT_TO_RECORD_BATCHES_OPTIONS),
      std::move(handler), false));

  response.mutable_init()->set_success(true);
  return response;
}

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
