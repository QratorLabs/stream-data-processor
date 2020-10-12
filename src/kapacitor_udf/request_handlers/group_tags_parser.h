#pragma once

#include <exception>
#include <regex>
#include <string>
#include <unordered_map>

class GroupParserException : public std::exception {};

class GroupTagsParser {
 public:
  static std::unordered_map<std::string, std::string> parse(
      const std::string& group_string);
};
