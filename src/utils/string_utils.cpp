#include <iterator>
#include <sstream>

#include "string_utils.h"

namespace stream_data_processor {
namespace string_utils {

std::vector<std::string> split(const std::string& str,
                               const std::string& delimiter) {
  if (str.empty()) {
    return std::vector<std::string>();
  }

  std::vector<std::string> parts;
  size_t last = 0;
  size_t next = 0;
  while ((next = str.find(delimiter, last)) != std::string::npos) {
    parts.push_back(str.substr(last, next - last));
    last = next + 1;
  }

  parts.push_back(str.substr(last));
  return parts;
}

std::string concatenateStrings(const std::vector<std::string>& parts,
                               const std::string& delimiter) {
  if (parts.empty()) {
    return "";
  }

  std::stringstream builder;
  for (size_t i = 0; i < parts.size() - 1; ++i) {
    builder << parts[i] << delimiter;
  }

  builder << parts.back();
  return builder.str();
}

std::string concatenateStrings(const std::unordered_set<std::string>& parts,
                               const std::string& delimiter) {
  if (parts.empty()) {
    return "";
  }

  std::stringstream builder;
  for (auto iter = parts.begin(); iter != parts.end(); ++iter) {
    builder << *iter;
    if (std::next(iter) != parts.end()) {
      builder << delimiter;
    }
  }

  return builder.str();
}

}  // namespace string_utils
}  // namespace stream_data_processor
