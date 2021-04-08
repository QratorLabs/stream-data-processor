#include <chrono>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include <spdlog/spdlog.h>

#include "invalid_option_exception.h"
#include "kapacitor_udf/utils/points_storage.h"
#include "record_batch_handlers/group_dispatcher.h"
#include "record_batch_handlers/stateful_handlers/threshold_state_machine.h"
#include "stateful_threshold_request_handler.h"

namespace stream_data_processor {
namespace kapacitor_udf {

namespace {

inline const std::string WATCH_COLUMN_OPTION_NAME{"watch"};
inline const std::string THRESHOLD_COLUMN_OPTION_NAME{"as"};
inline const std::string DEFAULT_THRESHOLD_OPTION_NAME{"defaultLevel"};
inline const std::string INCREASE_SCALE_OPTION_NAME{"increaseScaleFactor"};
inline const std::string INCREASE_AFTER_OPTION_NAME{"increaseAfter"};
inline const std::string DECREASE_SCALE_OPTION_NAME{"decreaseScaleFactor"};
inline const std::string DECREASE_TRIGGER_OPTION_NAME{
    "decreaseTriggerFactor"};
inline const std::string DECREASE_AFTER_OPTION_NAME{"decreaseAfter"};
inline const std::string MIN_LEVEL_OPTION_NAME{"minLevel"};
inline const std::string MAX_LEVEL_OPTION_NAME{"maxLevel"};

inline const std::unordered_set<std::string> REQUIRED_THRESHOLD_OPTIONS{
    WATCH_COLUMN_OPTION_NAME, THRESHOLD_COLUMN_OPTION_NAME,
    DEFAULT_THRESHOLD_OPTION_NAME, INCREASE_SCALE_OPTION_NAME,
    INCREASE_AFTER_OPTION_NAME};

inline const std::unordered_map<std::string, agent::ValueType>
    THRESHOLD_OPTIONS_TYPES{{WATCH_COLUMN_OPTION_NAME, agent::STRING},
                            {THRESHOLD_COLUMN_OPTION_NAME, agent::STRING},
                            {DEFAULT_THRESHOLD_OPTION_NAME, agent::DOUBLE},
                            {INCREASE_SCALE_OPTION_NAME, agent::DOUBLE},
                            {INCREASE_AFTER_OPTION_NAME, agent::DURATION},
                            {DECREASE_SCALE_OPTION_NAME, agent::DOUBLE},
                            {DECREASE_TRIGGER_OPTION_NAME, agent::DOUBLE},
                            {DECREASE_AFTER_OPTION_NAME, agent::DURATION},
                            {MIN_LEVEL_OPTION_NAME, agent::DOUBLE},
                            {MAX_LEVEL_OPTION_NAME, agent::DOUBLE}};

google::protobuf::Map<std::string, agent::OptionInfo>
getThresholdOptionsMap() {
  google::protobuf::Map<std::string, agent::OptionInfo> options_map;
  for (auto& [option_name, option_type] : THRESHOLD_OPTIONS_TYPES) {
    options_map[option_name].add_valuetypes(option_type);
  }

  return options_map;
}

ThresholdStateMachine::Options parseThresholdOptions(
    const google::protobuf::RepeatedPtrField<agent::Option>&
        request_options) {
  ThresholdStateMachine::Options threshold_options;
  std::unordered_set<std::string> parsed_options;
  for (auto& option : request_options) {
    auto& option_name = option.name();
    auto option_type = THRESHOLD_OPTIONS_TYPES.find(option_name);
    if (option_type == THRESHOLD_OPTIONS_TYPES.end()) {
      throw InvalidOptionException(
          fmt::format("Unexpected option name: {}", option_name));
    }

    if (parsed_options.find(option.name()) != parsed_options.end() ||
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
    if (option_name == WATCH_COLUMN_OPTION_NAME) {
      threshold_options.watch_column_name = option_value.stringvalue();
    } else if (option_name == THRESHOLD_COLUMN_OPTION_NAME) {
      threshold_options.threshold_column_name = option_value.stringvalue();
    } else if (option_name == DEFAULT_THRESHOLD_OPTION_NAME) {
      if (option_exact_type == agent::INT) {
        threshold_options.default_threshold = option_value.intvalue();
      } else {
        threshold_options.default_threshold = option_value.doublevalue();
      }
    } else if (option_name == INCREASE_SCALE_OPTION_NAME) {
      if (option_exact_type == agent::INT) {
        threshold_options.increase_scale_factor = option_value.intvalue();
      } else {
        threshold_options.increase_scale_factor = option_value.doublevalue();
      }
    } else if (option_name == INCREASE_AFTER_OPTION_NAME) {
      std::chrono::nanoseconds alert_duration(option_value.durationvalue());

      threshold_options.increase_after =
          std::chrono::duration_cast<std::chrono::seconds>(alert_duration);
    } else if (option_name == DECREASE_AFTER_OPTION_NAME) {
      std::chrono::nanoseconds decrease_duration(
          option_value.durationvalue());

      threshold_options.decrease_after =
          std::chrono::duration_cast<std::chrono::seconds>(decrease_duration);
    } else if (option_name == DECREASE_TRIGGER_OPTION_NAME) {
      threshold_options.decrease_trigger_factor = option_value.doublevalue();
    } else if (option_name == DECREASE_SCALE_OPTION_NAME) {
      threshold_options.decrease_scale_factor = option_value.doublevalue();
    } else if (option_name == MIN_LEVEL_OPTION_NAME) {
      threshold_options.min_threshold = option_value.doublevalue();
    } else if (option_name == MAX_LEVEL_OPTION_NAME) {
      threshold_options.max_threshold = option_value.doublevalue();
    } else {
      throw InvalidOptionException(
          fmt::format("Unexpected option name: {}", option_name));
    }
  }

  for (auto& required_option : REQUIRED_THRESHOLD_OPTIONS) {
    if (parsed_options.find(required_option) == parsed_options.end()) {
      throw InvalidOptionException(
          fmt::format("Missed required option: {}", required_option));
    }
  }

  threshold_options.threshold_column_type = metadata::FIELD;

  return threshold_options;
}

}  // namespace

const BasePointsConverter::PointsToRecordBatchesConversionOptions
    StatefulThresholdRequestHandler::DEFAULT_TO_RECORD_BATCHES_OPTIONS{
        "time", "name"};

StatefulThresholdRequestHandler::StatefulThresholdRequestHandler(
    const IUDFAgent* agent)
    : StreamRecordBatchRequestHandlerBase(agent) {}

agent::Response StatefulThresholdRequestHandler::info() const {
  agent::Response response;
  response.mutable_info()->set_wants(agent::STREAM);
  response.mutable_info()->set_provides(agent::STREAM);
  *response.mutable_info()->mutable_options() = getThresholdOptionsMap();

  return response;
}

agent::Response StatefulThresholdRequestHandler::init(
    const agent::InitRequest& init_request) {
  agent::Response response;
  ThresholdStateMachine::Options threshold_options;

  try {
    threshold_options = parseThresholdOptions(init_request.options());
  } catch (const InvalidOptionException& exc) {
    response.mutable_init()->set_success(false);
    response.mutable_init()->set_error(exc.what());
    return response;
  }

  std::unique_ptr<RecordBatchHandler> handler =
      std::make_unique<GroupDispatcher>(
          std::make_shared<ThresholdStateMachineFactory>(
              std::move(threshold_options)));

  setPointsStorage(std::make_unique<storage_utils::PointsStorage>(
      getAgent(),
      std::make_unique<BasePointsConverter>(
          DEFAULT_TO_RECORD_BATCHES_OPTIONS),
      std::move(handler), false));

  response.mutable_init()->set_success(true);
  return response;
}

void StatefulThresholdRequestHandler::point(const agent::Point& point) {
  getPointsStorage()->addPoint(point);
  handleBatch();
}

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
