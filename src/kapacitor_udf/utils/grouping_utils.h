#pragma once

#include <exception>
#include <string>
#include <unordered_map>

#include "metadata.pb.h"
#include "udf.pb.h"

namespace stream_data_processor {
namespace kapacitor_udf {
namespace grouping_utils {

using metadata::RecordBatchGroup;

class GroupParserException : public std::exception {};

RecordBatchGroup parse(const std::string& group_string,
                       const std::string& measurement_column_name);

RecordBatchGroup parse(const agent::Point& point,
                       const std::string& measurement_column_name);

std::string encode(
    const RecordBatchGroup& group, const std::string& measurement_column_name,
    const std::unordered_map<std::string, metadata::ColumnType> column_types);

}  // namespace grouping_utils
}  // namespace kapacitor_udf
}  // namespace stream_data_processor
