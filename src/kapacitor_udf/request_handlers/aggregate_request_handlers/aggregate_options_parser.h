#pragma once

#include <exception>
#include <regex>
#include <string>
#include <unordered_map>

#include <google/protobuf/map.h>
#include <google/protobuf/repeated_field.h>

#include "record_batch_handlers/aggregate_handler.h"

#include "udf.pb.h"

namespace stream_data_processor {
namespace kapacitor_udf {

class AggregateOptionsParser {
 public:
  [[nodiscard]] static google::protobuf::Map<std::string, agent::OptionInfo>
  getResponseOptionsMap();

  static void addResponseOptions(
      google::protobuf::Map<std::string, agent::OptionInfo>* options_map);

  static AggregateHandler::AggregateOptions parseOptions(
      const google::protobuf::RepeatedPtrField<agent::Option>&
          request_options);

 public:
  static const std::string AGGREGATES_OPTION_NAME;
  static const std::string TIME_AGGREGATE_RULE_OPTION_NAME;

 private:
  static void parseAggregates(
      const agent::Option& aggregates_request_option,
      AggregateHandler::AggregateOptions* aggregate_options);

  static void parseTimeAggregateRule(
      const agent::Option& time_aggregate_rule_option,
      AggregateHandler::AggregateOptions* aggregate_options);

 private:
  static const std::unordered_map<std::string,
                                  AggregateHandler::AggregateFunctionEnumType>
      FUNCTION_NAMES_TO_TYPES;
  static const std::regex AGGREGATE_STRING_REGEX;
};

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
