#pragma once

#include <exception>
#include <string>

#include "metadata.pb.h"
#include "udf.pb.h"

class GroupParserException : public std::exception {};

class GroupParser {
 public:
  static RecordBatchGroup parse(const std::string& group_string,
                                const std::string& measurement_column_name);

  static RecordBatchGroup parse(const agent::Point& point,
                                const std::string& measurement_column_name);

  static std::string encode(const RecordBatchGroup& group,
                            const std::string& measurement_column_name);
};
