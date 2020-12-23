#pragma once

#include <arrow/api.h>

namespace stream_data_processor {
namespace time_utils {

enum TimeUnit { NANO, MICRO, MILLI, SECOND, MINUTE, HOUR, DAY, WEEK };

arrow::Result<int64_t> convertTime(int64_t time, TimeUnit from, TimeUnit to);

TimeUnit mapArrowTimeUnit(arrow::TimeUnit::type arrow_time_unit);

}  // namespace time_utils
}  // namespace stream_data_processor
