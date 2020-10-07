#include "group_tags_parser.h"
#include "utils/string_utils.h"

std::unordered_map<std::string, std::string> GroupTagsParser::parse(const std::string &group_string) {
  std::vector<std::string> tag_values_strings;

  auto group_name_end_pos = group_string.find('\n');
  if (group_name_end_pos == std::string::npos) {
    tag_values_strings = StringUtils::split(group_string, ",");
  } else {
    tag_values_strings = StringUtils::split(group_string.substr(group_name_end_pos + 1), ",");
  }

  std::unordered_map<std::string, std::string> tag_values;
  for (auto& tag_value_string : tag_values_strings) {
    auto tag_value = StringUtils::split(tag_value_string, "=");
    if (tag_value.size() != 2) {
      throw GroupParserException();
    }

    tag_values[tag_value[0]] = tag_value[1];
  }

  return tag_values;
}
