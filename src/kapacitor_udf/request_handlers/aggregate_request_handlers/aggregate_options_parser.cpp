#include <spdlog/spdlog.h>

#include "aggregate_options_parser.h"
#include "kapacitor_udf/request_handlers/invalid_option_exception.h"

namespace stream_data_processor {
namespace kapacitor_udf {

const std::unordered_map<std::string,
                         AggregateHandler::AggregateFunctionEnumType>
    AggregateOptionsParser::FUNCTION_NAMES_TO_TYPES{
        {"min", AggregateHandler::AggregateFunctionEnumType::kMin},
        {"max", AggregateHandler::AggregateFunctionEnumType::kMax},
        {"first", AggregateHandler::AggregateFunctionEnumType::kFirst},
        {"last", AggregateHandler::AggregateFunctionEnumType::kLast},
        {"mean", AggregateHandler::AggregateFunctionEnumType::kMean},
    };

const std::string AggregateOptionsParser::AGGREGATES_OPTION_NAME{"aggregate"};
const std::string AggregateOptionsParser::TIME_AGGREGATE_RULE_OPTION_NAME{
    "timeAggregateRule"};
const std::regex AggregateOptionsParser::AGGREGATE_STRING_REGEX{
    R"((\S+)\((\w+)\)\s+as\s+(\S+))"};

google::protobuf::Map<std::string, agent::OptionInfo>
AggregateOptionsParser::getResponseOptionsMap() {
  google::protobuf::Map<std::string, agent::OptionInfo> options_map;
  options_map[AGGREGATES_OPTION_NAME].add_valuetypes(
      agent::ValueType::STRING);
  options_map[TIME_AGGREGATE_RULE_OPTION_NAME].add_valuetypes(
      agent::ValueType::STRING);
  return options_map;
}

AggregateHandler::AggregateOptions AggregateOptionsParser::parseOptions(
    const google::protobuf::RepeatedPtrField<agent::Option>&
        request_options) {
  AggregateHandler::AggregateOptions aggregate_options;

  for (auto& request_option : request_options) {
    if (request_option.name() == AGGREGATES_OPTION_NAME) {
      parseAggregates(request_option, &aggregate_options);
    } else if (request_option.name() == TIME_AGGREGATE_RULE_OPTION_NAME) {
      parseTimeAggregateRule(request_option, &aggregate_options);
    }
  }

  return aggregate_options;
}

void AggregateOptionsParser::parseAggregates(
    const agent::Option& aggregates_request_option,
    AggregateHandler::AggregateOptions* aggregate_options) {
  for (auto& aggregate_string_value : aggregates_request_option.values()) {
    std::smatch match;
    if (std::regex_match(aggregate_string_value.stringvalue(), match,
                         AGGREGATE_STRING_REGEX)) {
      if (FUNCTION_NAMES_TO_TYPES.find(match[1]) ==
          FUNCTION_NAMES_TO_TYPES.end()) {
        throw InvalidOptionException(fmt::format(
            "Invalid aggregate function name: {}", match[1].str()));
      }

      aggregate_options->aggregate_columns[match[2]].push_back(
          {FUNCTION_NAMES_TO_TYPES.at(match[1]), match[3]});
    } else {
      throw InvalidOptionException(fmt::format(
          "Invalid option value: {}", aggregate_string_value.stringvalue()));
    }
  }
}

void AggregateOptionsParser::parseTimeAggregateRule(
    const agent::Option& time_aggregate_rule_option,
    AggregateHandler::AggregateOptions* aggregate_options) {
  if (time_aggregate_rule_option.values_size() != 1) {
    throw InvalidOptionException(
        "timeAggregateRule option should accept exactly one argument");
  }

  auto time_aggregate_function_name =
      time_aggregate_rule_option.values(0).stringvalue();
  if (FUNCTION_NAMES_TO_TYPES.find(time_aggregate_function_name) ==
      FUNCTION_NAMES_TO_TYPES.end()) {
    throw InvalidOptionException(fmt::format(
        "Invalid aggregate function name: {}", time_aggregate_function_name));
  }

  aggregate_options->result_time_column_rule.aggregate_function =
      FUNCTION_NAMES_TO_TYPES.at(time_aggregate_function_name);
}

void AggregateOptionsParser::addResponseOptions(
    google::protobuf::Map<std::string, agent::OptionInfo>* options_map) {
  auto aggregate_options_map = getResponseOptionsMap();
  for (auto& [option_name, option_info] : aggregate_options_map) {
    (*options_map)[option_name] = option_info;
  }
}

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
