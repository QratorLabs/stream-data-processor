#include <map>
#include <sstream>

#include "group_parser.h"
#include "utils/string_utils.h"

#include "metadata/grouping.h"

RecordBatchGroup GroupParser::parse(
    const std::string& group_string,
    const std::string& measurement_column_name) {
  auto measurement_and_tags = StringUtils::split(group_string, "\n");
  if (measurement_and_tags.empty()) {
    return {};
  }

  if (measurement_and_tags.size() > 2) {
    throw GroupParserException();
  }

  std::map<std::string, std::string> group_map;
  size_t tags_index = 0;

  if (measurement_and_tags.size() > 1) {
    group_map[measurement_column_name] = measurement_and_tags[0];
    tags_index = 1;
  }

  auto tag_values_strings =
      StringUtils::split(measurement_and_tags[tags_index], ",");

  for (auto& tag_value_string : tag_values_strings) {
    auto tag_value = StringUtils::split(tag_value_string, "=");
    if (tag_value.size() != 2) {
      throw GroupParserException();
    }

    group_map[tag_value[0]] = tag_value[1];
  }

  return RecordBatchGrouping::constructGroupFromOrderedMap(group_map);
}

std::string GroupParser::encode(const RecordBatchGroup& group,
                                const std::string& measurement_column_name) {
  std::string measurement_prefix = "";
  std::stringstream tags_group_string_builder;
  bool first_tag_written = false;

  for (size_t i = 0; i < group.group_columns_values_size(); ++i) {
    auto& column_name = group.group_columns_names().columns_names(i);
    auto& column_value = group.group_columns_values(i);

    if (column_name == measurement_column_name) {
      measurement_prefix = column_value + '\n';
    } else {
      if (first_tag_written) {
        tags_group_string_builder << ',';
      }

      first_tag_written = true;
      tags_group_string_builder << column_name << '=' << column_value;
    }
  }

  return measurement_prefix + tags_group_string_builder.str();
}

RecordBatchGroup GroupParser::parse(
    const agent::Point& point, const std::string& measurement_column_name) {
  std::map<std::string, std::string> group_map;
  if (point.byname()) {
    group_map[measurement_column_name] = point.name();
  }

  for (auto& dimension : point.dimensions()) {
    if (point.tags().find(dimension) == point.tags().end()) {
      throw GroupParserException();
    }

    group_map[dimension] = point.tags().at(dimension);
  }

  return RecordBatchGrouping::constructGroupFromOrderedMap(group_map);
}
