#pragma once

#include <string>
#include <vector>

class StringUtils {
 public:
  static std::vector<std::string> split(const std::string& str,
                                        const std::string& delimiter);
  static std::string concatenateStrings(const std::vector<std::string>& parts,
                                        const std::string& delimiter = "");
};
