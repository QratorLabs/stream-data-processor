#pragma once

#include <string>
#include <unordered_set>
#include <vector>

namespace stream_data_processor {
namespace string_utils {

std::vector<std::string> split(const std::string& str,
                               const std::string& delimiter);
std::string concatenateStrings(const std::vector<std::string>& parts,
                               const std::string& delimiter = "");
std::string concatenateStrings(const std::unordered_set<std::string>& parts,
                               const std::string& delimiter = "");

}  // namespace string_utils
}  // namespace stream_data_processor
