#include <spdlog/spdlog.h>

#include "aggregate_options_parser.h"
#include "kapacitor_udf/request_handlers/invalid_option_exception.h"
#include "record_batch_handlers/record_batch_handlers.h"
#include "stream_aggregate_request_handler.h"

const PointsConverter::PointsToRecordBatchesConversionOptions
    StreamAggregateRequestHandler::DEFAULT_TO_RECORD_BATCHES_OPTIONS{"time",
                                                                     "name"};

const std::string StreamAggregateRequestHandler::TOLERANCE_OPTION_NAME{
    "tolerance"};

StreamAggregateRequestHandler::StreamAggregateRequestHandler(
    const std::shared_ptr<IUDFAgent>& agent)
    : StreamRecordBatchRequestHandlerBase(
          agent, DEFAULT_TO_RECORD_BATCHES_OPTIONS) {}

agent::Response StreamAggregateRequestHandler::info() const {
  agent::Response response;
  response.mutable_info()->set_wants(agent::EdgeType::STREAM);
  response.mutable_info()->set_provides(agent::EdgeType::STREAM);
  *response.mutable_info()->mutable_options() =
      AggregateOptionsParser::getResponseOptionsMap();

  response.mutable_info()
      ->mutable_options()
      ->
      operator[](TOLERANCE_OPTION_NAME)
      .add_valuetypes(agent::ValueType::DURATION);

  return response;
}

agent::Response StreamAggregateRequestHandler::init(
    const agent::InitRequest& init_request) {
  agent::Response response;
  AggregateHandler::AggregateOptions aggregate_options;

  try {
    aggregate_options =
        AggregateOptionsParser::parseOptions(init_request.options());

    parseToleranceOption(init_request.options());
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

agent::Response StreamAggregateRequestHandler::snapshot() const {
  agent::Response response;

  response.mutable_snapshot()->set_snapshot(
      batch_points_.SerializeAsString());

  return response;
}

agent::Response StreamAggregateRequestHandler::restore(
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

void StreamAggregateRequestHandler::point(const agent::Point& point) {
  if (needHandleBefore(point)) {
    handleBatch();
  }

  auto new_point = batch_points_.mutable_points()->Add();
  new_point->CopyFrom(point);
}

void StreamAggregateRequestHandler::parseToleranceOption(
    const google::protobuf::RepeatedPtrField<agent::Option>&
        request_options) {
  bool found_tolerance_option = false;
  for (auto& option : request_options) {
    if (option.name() == TOLERANCE_OPTION_NAME) {
      if (found_tolerance_option) {
        throw InvalidOptionException(
            "Doubled tolerance option is prohibited");
      } else {
        found_tolerance_option = true;
      }

      if (option.values_size() != 1) {
        throw InvalidOptionException(fmt::format(
            "tolerance option should accept exactly one argument, "
            "got {} instead",
            option.values_size()));
      }

      tolerance_ = std::chrono::nanoseconds(option.values(0).durationvalue());
    }
  }
}

bool StreamAggregateRequestHandler::needHandleBefore(
    const agent::Point& point) const {
  if (batch_points_.points().empty()) {
    return false;
  }

  if (batch_points_.points(0).time() >= point.time()) {
    return false;
  }

  if (std::chrono::milliseconds(
          point.time() - batch_points_.points(0).time()) < tolerance_) {
    return false;
  }

  return true;
}
