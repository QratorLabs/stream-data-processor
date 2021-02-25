#include <spdlog/spdlog.h>

#include "aggregate_options_parser.h"
#include "kapacitor_udf/request_handlers/invalid_option_exception.h"
#include "record_batch_handlers/record_batch_handlers.h"
#include "stream_aggregate_request_handler.h"

namespace stream_data_processor {
namespace kapacitor_udf {

namespace {

inline const std::string EMIT_TIMEOUT_OPTION_NAME{"emitTimeout"};
inline const std::string TOLERANCE_OPTION_NAME{"tolerance"};

inline const std::unordered_set<std::string>
    REQUIRED_STREAM_AGGREGATE_OPTIONS{EMIT_TIMEOUT_OPTION_NAME};

inline const std::chrono::seconds DEFAULT_TOLERANCE_VALUE{0};

inline const std::unordered_map<std::string, agent::ValueType>
    WINDOW_OPTIONS_TYPES{{EMIT_TIMEOUT_OPTION_NAME, agent::DURATION},
                         {TOLERANCE_OPTION_NAME, agent::DURATION}};

google::protobuf::Map<std::string, agent::OptionInfo>
getStreamAggregateOptionsMap() {
  google::protobuf::Map<std::string, agent::OptionInfo> options_map;
  for (auto& [option_name, option_type] : WINDOW_OPTIONS_TYPES) {
    options_map[option_name].add_valuetypes(option_type);
  }

  return options_map;
}

[[nodiscard]] std::unordered_map<std::string, std::chrono::seconds>
parseStreamAggregateOptions(
    const google::protobuf::RepeatedPtrField<agent::Option>&
        request_options) {
  std::unordered_map<std::string, std::chrono::seconds> options;
  std::unordered_set<std::string> parsed_options;
  for (auto& option : request_options) {
    auto& option_name = option.name();
    auto option_type = WINDOW_OPTIONS_TYPES.find(option_name);
    if (option_type == WINDOW_OPTIONS_TYPES.end()) {
      continue;
    }

    if (parsed_options.find(option_name) != parsed_options.end() ||
        option.values_size() != 1) {
      throw InvalidOptionException(fmt::format(
          "Expected exactly one value of option {}", option_name));
    }

    auto& option_value = option.values(0);
    auto option_exact_type = option_value.type();

    if (option_exact_type != option_type->second) {
      throw InvalidOptionException(
          fmt::format("Unexpected type {} of option {}",
                      agent::ValueType_Name(option_exact_type), option_name));
    }

    parsed_options.insert(option_name);
    std::chrono::nanoseconds duration_value(option_value.durationvalue());

    options[option_name] =
        std::chrono::duration_cast<std::chrono::seconds>(duration_value);
  }

  for (auto& required_option : REQUIRED_STREAM_AGGREGATE_OPTIONS) {
    if (parsed_options.find(required_option) == parsed_options.end()) {
      throw InvalidOptionException(
          fmt::format("Missed required option: {}", required_option));
    }
  }

  if (options.find(TOLERANCE_OPTION_NAME) == options.end()) {
    options[TOLERANCE_OPTION_NAME] = DEFAULT_TOLERANCE_VALUE;
  }

  ++options[TOLERANCE_OPTION_NAME];

  return options;
}

}  // namespace

const BasePointsConverter::PointsToRecordBatchesConversionOptions
    StreamAggregateRequestHandler::DEFAULT_TO_RECORD_BATCHES_OPTIONS{"time",
                                                                     "name"};

StreamAggregateRequestHandler::StreamAggregateRequestHandler(
    const IUDFAgent* agent, uvw::Loop* loop)
    : TimerRecordBatchRequestHandlerBase(agent, loop) {}

agent::Response StreamAggregateRequestHandler::info() const {
  agent::Response response;
  response.mutable_info()->set_wants(agent::EdgeType::STREAM);
  response.mutable_info()->set_provides(agent::EdgeType::STREAM);
  *response.mutable_info()->mutable_options() =
      getStreamAggregateOptionsMap();

  AggregateOptionsParser::addResponseOptions(
      response.mutable_info()->mutable_options());

  return response;
}

agent::Response StreamAggregateRequestHandler::init(
    const agent::InitRequest& init_request) {
  agent::Response response;
  AggregateHandler::AggregateOptions aggregate_options;

  std::unordered_map<std::string, std::chrono::seconds>
      stream_aggregate_options;

  try {
    aggregate_options =
        AggregateOptionsParser::parseOptions(init_request.options());

    stream_aggregate_options =
        parseStreamAggregateOptions(init_request.options());
  } catch (const InvalidOptionException& exc) {
    response.mutable_init()->set_success(false);
    response.mutable_init()->set_error(exc.what());
    return response;
  }

  setEmitTimeout(stream_aggregate_options[EMIT_TIMEOUT_OPTION_NAME]);

  auto pipeline_handler = std::make_unique<PipelineHandler>();

  WindowHandler::WindowOptions window_options{
      stream_aggregate_options[TOLERANCE_OPTION_NAME],
      stream_aggregate_options[TOLERANCE_OPTION_NAME], true};

  pipeline_handler->pushBackHandler(
      std::make_shared<WindowHandler>(std::move(window_options)));

  pipeline_handler->pushBackHandler(
      std::make_shared<AggregateHandler>(std::move(aggregate_options)));

  setPointsStorage(std::make_unique<storage_utils::PointsStorage>(
      getAgent(),
      std::make_unique<BasePointsConverter>(
          DEFAULT_TO_RECORD_BATCHES_OPTIONS),
      std::move(pipeline_handler), false));

  response.mutable_init()->set_success(true);
  return response;
}

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
