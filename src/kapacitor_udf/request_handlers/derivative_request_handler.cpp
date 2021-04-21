#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include <spdlog/spdlog.h>

#include "derivative_request_handler.h"
#include "invalid_option_exception.h"
#include "kapacitor_udf/utils/points_converter.h"
#include "record_batch_handlers/record_batch_handlers.h"

namespace stream_data_processor {
namespace kapacitor_udf {

namespace {

inline const std::string DERIVATIVE_OPTION_NAME{"derivative"};
inline const std::string RESULT_OPTION_NAME{"as"};
inline const std::string ORDER_OPTION_NAME{"order"};
inline const std::string UNIT_TIME_SEGMENT_OPTION_NAME{"unit"};
inline const std::string NEIGHBOURHOOD_OPTION_NAME{"neighbourhood"};
inline const std::string EMIT_TIMEOUT_OPTION_NAME{"emitTimeout"};
inline const std::string NO_WAIT_FUTURE_OPTION_NAME{"noWait"};

inline const std::unordered_set<std::string> REQUIRED_DERIVATIVE_CASE_OPTIONS{
    DERIVATIVE_OPTION_NAME, RESULT_OPTION_NAME};

inline const std::unordered_map<std::string, agent::ValueType>
    DERIVATIVE_OPTIONS_TYPES{{DERIVATIVE_OPTION_NAME, agent::STRING},
                             {RESULT_OPTION_NAME, agent::STRING},
                             {ORDER_OPTION_NAME, agent::INT},
                             {UNIT_TIME_SEGMENT_OPTION_NAME, agent::DURATION},
                             {NEIGHBOURHOOD_OPTION_NAME, agent::DURATION},
                             {EMIT_TIMEOUT_OPTION_NAME, agent::DURATION}};

inline const std::unordered_set<std::string> OPTIONAL_GLOBAL_OPTIONS{
    UNIT_TIME_SEGMENT_OPTION_NAME, NEIGHBOURHOOD_OPTION_NAME,
    EMIT_TIMEOUT_OPTION_NAME};

using namespace std::chrono_literals;

inline const size_t DEFAULT_ORDER_OPTION_VALUE{1};

inline const std::chrono::nanoseconds DEFAULT_UNIT_TIME_SEGMENT_OPTION_VALUE{
    1s};

inline const std::chrono::seconds DEFAULT_EMIT_TIMEOUT_OPTION_VALUE{10s};

void addDerivativeCase(
    std::string result_column_name,
    DerivativeHandler::DerivativeCase&& derivative_case,
    std::unordered_set<std::string>* parsed_derivative_case_options,
    std::unordered_map<std::string, DerivativeHandler::DerivativeCase>*
        derivative_cases) {
  for (auto& required_option : REQUIRED_DERIVATIVE_CASE_OPTIONS) {
    if (parsed_derivative_case_options->find(required_option) ==
        parsed_derivative_case_options->end()) {
      throw InvalidOptionException(fmt::format(
          "Missing required derivative case option: {}", required_option));
    }
  }

  if (derivative_cases->find(result_column_name) != derivative_cases->end()) {
    throw InvalidOptionException(fmt::format(
        "Duplicating result fields with name: {}", result_column_name));
  }

  (*derivative_cases)[result_column_name] = std::move(derivative_case);
  if (parsed_derivative_case_options->find(ORDER_OPTION_NAME) ==
      parsed_derivative_case_options->end()) {
    (*derivative_cases)[result_column_name].order =
        DEFAULT_ORDER_OPTION_VALUE;
  }

  parsed_derivative_case_options->clear();
}

}  // namespace

namespace internal {

google::protobuf::Map<std::string, agent::OptionInfo>
getDerivativeOptionsMap() {
  google::protobuf::Map<std::string, agent::OptionInfo> options_map;
  for (auto& [option_name, option_type] : DERIVATIVE_OPTIONS_TYPES) {
    options_map[option_name].add_valuetypes(option_type);
  }

  options_map[NO_WAIT_FUTURE_OPTION_NAME].Clear();

  return options_map;
}

DerivativeOptions parseDerivativeOptions(
    const google::protobuf::RepeatedPtrField<agent::Option>&
        request_options) {
  DerivativeOptions derivative_options;
  std::string current_result_option_value;
  DerivativeHandler::DerivativeCase current_derivative_case;
  std::unordered_set<std::string> parsed_derivative_case_options;
  std::unordered_set<std::string> parsed_global_options;
  for (auto& option : request_options) {
    auto& option_name = option.name();
    auto option_type = DERIVATIVE_OPTIONS_TYPES.find(option_name);
    if (option_type == DERIVATIVE_OPTIONS_TYPES.end() &&
        option_name != NO_WAIT_FUTURE_OPTION_NAME) {
      throw InvalidOptionException(
          fmt::format("Unexpected option name: {}", option_name));
    }

    if (parsed_global_options.find(option.name()) !=
            parsed_global_options.end() ||
        option.values_size() > 1) {
      throw InvalidOptionException(fmt::format(
          "Expected at most one value of option {}", option_name));
    }

    if (option_name == NO_WAIT_FUTURE_OPTION_NAME) {
      if (option.values_size() > 0) {
        throw InvalidOptionException(fmt::format(
            "Option {} is supposed not to have values", option_name));
      }

      derivative_options.options.no_wait_future = true;
      continue;
    }

    if (option.values_size() == 0) {
      throw InvalidOptionException(fmt::format(
          "Option {} supposed to have value of type {}", option_name,
          agent::ValueType_Name(option_type->second)));
    }

    auto& option_value = option.values(0);
    auto option_exact_type = option_value.type();

    if (option_exact_type != option_type->second) {
      throw InvalidOptionException(
          fmt::format("Unexpected type {} of option {}",
                      agent::ValueType_Name(option_exact_type), option_name));
    }

    if (OPTIONAL_GLOBAL_OPTIONS.find(option_name) !=
        OPTIONAL_GLOBAL_OPTIONS.end()) {
      if (!parsed_derivative_case_options.empty()) {
        addDerivativeCase(current_result_option_value,
                          std::move(current_derivative_case),
                          &parsed_derivative_case_options,
                          &derivative_options.options.derivative_cases);
      }

      parsed_global_options.insert(option_name);
      if (option_name == UNIT_TIME_SEGMENT_OPTION_NAME) {
        derivative_options.options.unit_time_segment =
            std::chrono::nanoseconds(option_value.durationvalue());
      } else if (option_name == NEIGHBOURHOOD_OPTION_NAME) {
        derivative_options.options.derivative_neighbourhood =
            std::chrono::nanoseconds(option_value.durationvalue());
      } else if (option_name == EMIT_TIMEOUT_OPTION_NAME) {
        std::chrono::nanoseconds emit_timeout(option_value.durationvalue());

        derivative_options.emit_timeout =
            std::chrono::duration_cast<std::chrono::seconds>(emit_timeout);
      } else {
        throw InvalidOptionException(
            fmt::format("Unexpected option name: {}", option_name));
      }
    } else {
      if (option_name == DERIVATIVE_OPTION_NAME) {
        if (!parsed_derivative_case_options.empty()) {
          addDerivativeCase(current_result_option_value,
                            std::move(current_derivative_case),
                            &parsed_derivative_case_options,
                            &derivative_options.options.derivative_cases);
        }

        current_derivative_case.values_column_name =
            option_value.stringvalue();
      } else {
        if (parsed_derivative_case_options.find(DERIVATIVE_OPTION_NAME) ==
            parsed_derivative_case_options.end()) {
          throw InvalidOptionException(
              fmt::format("Expected \"{}\" option to be passed before \"{}\" "
                          "option",
                          DERIVATIVE_OPTION_NAME, option_name));
        }

        if (option_name == RESULT_OPTION_NAME) {
          current_result_option_value = option_value.stringvalue();
        } else if (option_name == ORDER_OPTION_NAME) {
          if (option_value.intvalue() < 0) {
            throw InvalidOptionException(
                fmt::format("Non-negative value of option {} were expected, "
                            "got: {}",
                            option_name, option_value.intvalue()));
          }

          current_derivative_case.order = option_value.intvalue();
        } else {
          throw InvalidOptionException(
              fmt::format("Unexpected option name: {}", option_name));
        }
      }

      parsed_derivative_case_options.insert(option_name);
    }
  }

  if (!parsed_derivative_case_options.empty()) {
    addDerivativeCase(current_result_option_value,
                      std::move(current_derivative_case),
                      &parsed_derivative_case_options,
                      &derivative_options.options.derivative_cases);
  }

  if (derivative_options.options.derivative_cases.empty()) {
    throw InvalidOptionException("Expected at least one derivative case");
  }

  if (parsed_global_options.find(UNIT_TIME_SEGMENT_OPTION_NAME) ==
      parsed_global_options.end()) {
    derivative_options.options.unit_time_segment =
        DEFAULT_UNIT_TIME_SEGMENT_OPTION_VALUE;
  }

  if (parsed_global_options.find(NEIGHBOURHOOD_OPTION_NAME) ==
      parsed_global_options.end()) {
    derivative_options.options.derivative_neighbourhood =
        derivative_options.options.unit_time_segment;
  }

  if (parsed_global_options.find(EMIT_TIMEOUT_OPTION_NAME) ==
      parsed_global_options.end()) {
    derivative_options.emit_timeout = DEFAULT_EMIT_TIMEOUT_OPTION_VALUE;
  }

  return derivative_options;
}

}  // namespace internal

const BasePointsConverter::PointsToRecordBatchesConversionOptions
    DerivativeRequestHandler::DEFAULT_TO_RECORD_BATCHES_OPTIONS{"time",
                                                                "name"};

DerivativeRequestHandler::DerivativeRequestHandler(const IUDFAgent* agent,
                                                   uvw::Loop* loop)
    : TimerRecordBatchRequestHandlerBase(agent, loop) {}

agent::Response DerivativeRequestHandler::info() const {
  agent::Response response;
  response.mutable_info()->set_wants(agent::STREAM);
  response.mutable_info()->set_provides(agent::STREAM);

  *response.mutable_info()->mutable_options() =
      internal::getDerivativeOptionsMap();

  return response;
}

agent::Response DerivativeRequestHandler::init(
    const agent::InitRequest& init_request) {
  agent::Response response;
  internal::DerivativeOptions derivative_options;

  try {
    derivative_options =
        internal::parseDerivativeOptions(init_request.options());
  } catch (const InvalidOptionException& exc) {
    response.mutable_init()->set_success(false);
    response.mutable_init()->set_error(exc.what());
    return response;
  }

  setEmitTimeout(derivative_options.emit_timeout);

  std::unique_ptr<RecordBatchHandler> handler =
      std::make_unique<GroupDispatcher>(
          std::make_shared<DerivativeHandlerFactory>(
              std::make_unique<compute_utils::FDDerivativeCalculator>(),
              std::move(derivative_options.options)));

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
