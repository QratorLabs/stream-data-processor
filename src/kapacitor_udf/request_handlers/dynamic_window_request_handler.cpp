#include <sstream>

#include <spdlog/spdlog.h>

#include "dynamic_window_request_handler.h"
#include "invalid_option_exception.h"
#include "metadata/time_metadata.h"
#include "record_batch_handlers/group_dispatcher.h"
#include "utils/string_utils.h"

namespace stream_data_processor {
namespace kapacitor_udf {

namespace {

inline const std::string PERIOD_FIELD_OPTION_NAME{"periodField"};
inline const std::string PERIOD_TIME_UNIT_OPTION_NAME{"periodTimeUnit"};
inline const std::string EVERY_FIELD_OPTION_NAME{"everyField"};
inline const std::string EVERY_TIME_UNIT_OPTION_NAME{"everyTimeUnit"};
inline const std::string FILL_PERIOD_OPTION_NAME{"fillPeriod"};
inline const std::string DEFAULT_PERIOD_OPTION_NAME{"defaultPeriod"};
inline const std::string DEFAULT_EVERY_OPTION_NAME{"defaultEvery"};
inline const std::string EMIT_TIMEOUT_OPTION_NAME{"emitTimeout"};
inline const std::string STATIC_PERIOD_OPTION_NAME{"staticPeriod"};
inline const std::string STATIC_EVERY_OPTION_NAME{"staticEvery"};

inline const std::unordered_map<std::string, agent::ValueType>
    WINDOW_OPTIONS_TYPES{{PERIOD_FIELD_OPTION_NAME, agent::STRING},
                         {PERIOD_TIME_UNIT_OPTION_NAME, agent::STRING},
                         {EVERY_FIELD_OPTION_NAME, agent::STRING},
                         {EVERY_TIME_UNIT_OPTION_NAME, agent::STRING},
                         {DEFAULT_PERIOD_OPTION_NAME, agent::DURATION},
                         {DEFAULT_EVERY_OPTION_NAME, agent::DURATION},
                         {EMIT_TIMEOUT_OPTION_NAME, agent::DURATION},
                         {STATIC_PERIOD_OPTION_NAME, agent::DURATION},
                         {STATIC_EVERY_OPTION_NAME, agent::DURATION}};

inline const std::unordered_map<std::string, int> OPTIONS_SIZE{
    {PERIOD_FIELD_OPTION_NAME, 1},  {PERIOD_TIME_UNIT_OPTION_NAME, 1},
    {EVERY_FIELD_OPTION_NAME, 1},   {EVERY_TIME_UNIT_OPTION_NAME, 1},
    {FILL_PERIOD_OPTION_NAME, 0},   {DEFAULT_PERIOD_OPTION_NAME, 1},
    {DEFAULT_EVERY_OPTION_NAME, 1}, {EMIT_TIMEOUT_OPTION_NAME, 1},
    {STATIC_PERIOD_OPTION_NAME, 1}, {STATIC_EVERY_OPTION_NAME, 1}};

inline const std::vector<std::vector<std::unordered_set<std::string>>>
    PRESENTED_OPTIONS_EXCLUSIVE_CNF{
        {{PERIOD_FIELD_OPTION_NAME, PERIOD_TIME_UNIT_OPTION_NAME,
          DEFAULT_PERIOD_OPTION_NAME},
         {STATIC_PERIOD_OPTION_NAME}},
        {{EVERY_FIELD_OPTION_NAME, EVERY_TIME_UNIT_OPTION_NAME,
          DEFAULT_EVERY_OPTION_NAME},
         {STATIC_EVERY_OPTION_NAME}},
        {{EMIT_TIMEOUT_OPTION_NAME}}};

inline const std::unordered_map<std::string, time_utils::TimeUnit>
    TIME_NAMES_TO_UNITS{{"ns", time_utils::NANO},   {"u", time_utils::MICRO},
                        {"mcs", time_utils::MICRO}, {"us", time_utils::MICRO},
                        {"ms", time_utils::MILLI},  {"s", time_utils::SECOND},
                        {"m", time_utils::MINUTE},  {"h", time_utils::HOUR},
                        {"d", time_utils::DAY},     {"w", time_utils::WEEK}};

}  // namespace

namespace internal {

arrow::Result<arrow::RecordBatchVector>
WindowOptionsConverterDecorator::convertToRecordBatches(
    const agent::PointBatch& points) const {
  arrow::RecordBatchVector converted_record_batches;
  ARROW_ASSIGN_OR_RAISE(
      converted_record_batches,
      BasePointsConverterDecorator::convertToRecordBatches(points));

  for (auto& record_batch : converted_record_batches) {
    if (options_.every_option.has_value()) {
      if (record_batch->GetColumnByName(
              options_.every_option.value().first) != nullptr) {
        ARROW_RETURN_NOT_OK(metadata::setTimeUnitMetadata(
            &record_batch, options_.every_option.value().first,
            options_.every_option.value().second));
      }
    }

    if (options_.period_option.has_value()) {
      if (record_batch->GetColumnByName(
              options_.period_option.value().first) != nullptr) {
        ARROW_RETURN_NOT_OK(metadata::setTimeUnitMetadata(
            &record_batch, options_.period_option.value().first,
            options_.period_option.value().second));
      }
    }
  }

  return converted_record_batches;
}

google::protobuf::Map<std::string, agent::OptionInfo> getWindowOptionsMap() {
  google::protobuf::Map<std::string, agent::OptionInfo> options_map;
  for (auto& [option_name, option_type] : WINDOW_OPTIONS_TYPES) {
    if (OPTIONS_SIZE.at(option_name) > 0) {
      options_map[option_name].add_valuetypes(option_type);
    }
  }

  for (auto& [option_name, minimal_size] : OPTIONS_SIZE) {
    if (minimal_size == 0) {
      options_map[option_name].Clear();
    }
  }

  return options_map;
}

std::string constructErrorMessageForUnsatisfiedExclusiveOptionDisjunction(
    const std::vector<std::unordered_set<std::string>>&
        exclusive_disjunction) {
  std::stringstream error_message;
  error_message << "Expected exactly one of the following option sets to be "
                   "presented:\n";
  for (auto& option_set : exclusive_disjunction) {
    error_message << "{ "
                  << string_utils::concatenateStrings(option_set, ", ")
                  << " }\n";
  }

  return error_message.str();
}

WindowOptions parseWindowOptions(
    const google::protobuf::RepeatedPtrField<agent::Option>&
        request_options) {
  WindowOptions window_options;
  window_options.window_handler_options.fill_period = false;

  std::unordered_map<std::string, int> parsed_options;
  for (auto& option : request_options) {
    auto& option_name = option.name();
    if (OPTIONS_SIZE.find(option_name) == OPTIONS_SIZE.end()) {
      throw InvalidOptionException(
          fmt::format("Unexpected option name: {}", option_name));
    }

    if (parsed_options.find(option_name) == parsed_options.end()) {
      parsed_options[option_name] = 0;
    }

    parsed_options[option_name] += option.values_size();
    if (option.values_size() == 0) {
      if (option_name == FILL_PERIOD_OPTION_NAME) {
        window_options.window_handler_options.fill_period = true;
      } else {
        throw InvalidOptionException(
            fmt::format("Unexpected option name: {}", option_name));
      }

      continue;
    }

    if (parsed_options[option_name] > OPTIONS_SIZE.at(option_name)) {
      throw InvalidOptionException(
          fmt::format("Expected not more than {} values of option {}",
                      OPTIONS_SIZE.at(option_name), option_name));
    }

    auto& option_value = option.values(0);
    auto option_exact_type = option_value.type();

    if (option_exact_type != WINDOW_OPTIONS_TYPES.at(option_name)) {
      throw InvalidOptionException(
          fmt::format("Unexpected type {} of option {}",
                      agent::ValueType_Name(option_exact_type), option_name));
    }

    if (option_name == PERIOD_FIELD_OPTION_NAME) {
      if (!window_options.convert_options.period_option.has_value()) {
        window_options.convert_options.period_option.emplace();
      }

      window_options.convert_options.period_option.value().first =
          option_value.stringvalue();
    } else if (option_name == PERIOD_TIME_UNIT_OPTION_NAME) {
      if (!window_options.convert_options.period_option.has_value()) {
        window_options.convert_options.period_option.emplace();
      }

      if (TIME_NAMES_TO_UNITS.find(option_value.stringvalue()) ==
          TIME_NAMES_TO_UNITS.end()) {
        throw InvalidOptionException(
            fmt::format("Unexpected time unit shortcut: \"{}\"",
                        option_value.stringvalue()));
      }

      window_options.convert_options.period_option.value().second =
          TIME_NAMES_TO_UNITS.at(option_value.stringvalue());
    } else if (option_name == EVERY_FIELD_OPTION_NAME) {
      if (!window_options.convert_options.every_option.has_value()) {
        window_options.convert_options.every_option.emplace();
      }

      window_options.convert_options.every_option.value().first =
          option_value.stringvalue();
    } else if (option_name == EVERY_TIME_UNIT_OPTION_NAME) {
      if (!window_options.convert_options.every_option.has_value()) {
        window_options.convert_options.every_option.emplace();
      }

      if (TIME_NAMES_TO_UNITS.find(option_value.stringvalue()) ==
          TIME_NAMES_TO_UNITS.end()) {
        throw InvalidOptionException(
            fmt::format("Unexpected time unit shortcut: \"{}\"",
                        option_value.stringvalue()));
      }

      window_options.convert_options.every_option.value().second =
          TIME_NAMES_TO_UNITS.at(option_value.stringvalue());
    } else if (option_name == DEFAULT_PERIOD_OPTION_NAME) {
      std::chrono::nanoseconds default_period(option_value.durationvalue());

      window_options.window_handler_options.period =
          std::chrono::duration_cast<std::chrono::seconds>(default_period);
    } else if (option_name == DEFAULT_EVERY_OPTION_NAME) {
      std::chrono::nanoseconds default_every(option_value.durationvalue());

      window_options.window_handler_options.every =
          std::chrono::duration_cast<std::chrono::seconds>(default_every);
    } else if (option_name == EMIT_TIMEOUT_OPTION_NAME) {
      std::chrono::nanoseconds emit_timeout(option_value.durationvalue());

      window_options.emit_timeout =
          std::chrono::duration_cast<std::chrono::seconds>(emit_timeout);
    } else if (option_name == STATIC_PERIOD_OPTION_NAME) {
      std::chrono::nanoseconds static_period(option_value.durationvalue());

      window_options.window_handler_options.period =
          std::chrono::duration_cast<std::chrono::seconds>(static_period);
    } else if (option_name == STATIC_EVERY_OPTION_NAME) {
      std::chrono::nanoseconds static_every(option_value.durationvalue());

      window_options.window_handler_options.every =
          std::chrono::duration_cast<std::chrono::seconds>(static_every);
    } else {
      throw InvalidOptionException(
          fmt::format("Unexpected option name: {}", option_name));
    }
  }

  for (auto& exclusive_disjunction : PRESENTED_OPTIONS_EXCLUSIVE_CNF) {
    bool is_disjunction_satisfied = false;
    for (auto& option_set : exclusive_disjunction) {
      bool is_set_presented = true;
      for (auto& option : option_set) {
        if (parsed_options.find(option) == parsed_options.end()) {
          is_set_presented = false;
          break;
        }
      }

      if (is_set_presented) {
        if (is_disjunction_satisfied) {
          throw InvalidOptionException(
              constructErrorMessageForUnsatisfiedExclusiveOptionDisjunction(
                  exclusive_disjunction));
        } else {
          is_disjunction_satisfied = true;
        }
      }
    }

    if (!is_disjunction_satisfied) {
      throw InvalidOptionException(
          constructErrorMessageForUnsatisfiedExclusiveOptionDisjunction(
              exclusive_disjunction));
    }
  }

  return window_options;
}

}  // namespace internal

const BasePointsConverter::PointsToRecordBatchesConversionOptions
    DynamicWindowRequestHandler::DEFAULT_TO_RECORD_BATCHES_OPTIONS{"time",
                                                                   "name"};

DynamicWindowRequestHandler::DynamicWindowRequestHandler(
    const IUDFAgent* agent, uvw::Loop* loop)
    : TimerRecordBatchRequestHandlerBase(agent, loop) {}

agent::Response DynamicWindowRequestHandler::info() const {
  agent::Response response;
  response.mutable_info()->set_wants(agent::STREAM);
  response.mutable_info()->set_provides(agent::BATCH);

  *response.mutable_info()->mutable_options() =
      internal::getWindowOptionsMap();

  return response;
}

agent::Response DynamicWindowRequestHandler::init(
    const agent::InitRequest& init_request) {
  agent::Response response;
  internal::WindowOptions window_options;

  try {
    window_options = internal::parseWindowOptions(init_request.options());
  } catch (const InvalidOptionException& exc) {
    response.mutable_init()->set_success(false);
    response.mutable_init()->set_error(exc.what());
    return response;
  }

  setEmitTimeout(window_options.emit_timeout);

  DynamicWindowHandler::DynamicWindowOptions dynamic_window_options;
  if (window_options.convert_options.period_option.has_value()) {
    dynamic_window_options.period_column_name =
        window_options.convert_options.period_option.value().first;
  }
  if (window_options.convert_options.every_option.has_value()) {
    dynamic_window_options.every_column_name =
        window_options.convert_options.every_option.value().first;
  }

  std::unique_ptr<RecordBatchHandler> handler =
      std::make_unique<GroupDispatcher>(
          std::make_shared<DynamicWindowHandlerFactory>(
              window_options.window_handler_options,
              std::move(dynamic_window_options)));

  setPointsStorage(std::make_unique<storage_utils::PointsStorage>(
      getAgent(),
      std::make_unique<internal::WindowOptionsConverterDecorator>(
          std::make_shared<BasePointsConverter>(
              DEFAULT_TO_RECORD_BATCHES_OPTIONS),
          window_options.convert_options),
      std::move(handler), true));

  response.mutable_init()->set_success(true);
  return response;
}

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
