#include <utility>

#include <spdlog/spdlog.h>

#include "time_utils.h"

namespace stream_data_processor {
namespace time_utils {

namespace {

inline constexpr int64_t SECONDS_IN_DAY = 86400;
inline constexpr int64_t SECONDS_IN_WEEK = 604800;

template <class ToDuration, class Rep, class Period>
arrow::Result<ToDuration> checkedConvert(
    const std::chrono::duration<Rep, Period>& from) {
  using DoubledDuration =
      std::chrono::duration<double, typename ToDuration::period>;

  constexpr DoubledDuration min = ToDuration::min();
  constexpr DoubledDuration max = ToDuration::max();
  DoubledDuration doubled_duration = from;
  if (doubled_duration < min || doubled_duration > max) {
    return arrow::Status::TypeError("Overflow while converting durations");
  }

  return std::chrono::duration_cast<ToDuration>(doubled_duration);
}

template <class Duration>
arrow::Result<int64_t> arrowReturnOrRaise(
    const arrow::Result<Duration>& duration_result) {
  ARROW_RETURN_NOT_OK(duration_result.status());
  return duration_result.ValueOrDie().count();
}

template <class Rep, class Period>
arrow::Result<int64_t> convertToTimeUnit(
    const std::chrono::duration<Rep, Period>& from, TimeUnit to) {
  switch (to) {
    case NANO:
      return arrowReturnOrRaise(
          checkedConvert<std::chrono::nanoseconds>(from));
    case MICRO:
      return arrowReturnOrRaise(
          checkedConvert<std::chrono::microseconds>(from));
    case MILLI:
      return arrowReturnOrRaise(
          checkedConvert<std::chrono::milliseconds>(from));
    case SECOND:
      return arrowReturnOrRaise(checkedConvert<std::chrono::seconds>(from));
    case MINUTE:
      return arrowReturnOrRaise(checkedConvert<std::chrono::minutes>(from));
    case HOUR:
      return arrowReturnOrRaise(checkedConvert<std::chrono::hours>(from));
    case DAY:
      return arrowReturnOrRaise(
          checkedConvert<
              std::chrono::duration<int64_t, std::ratio<SECONDS_IN_DAY>>>(
              from));
    case WEEK:
      return arrowReturnOrRaise(
          checkedConvert<
              std::chrono::duration<int64_t, std::ratio<SECONDS_IN_WEEK>>>(
              from));
    default:
      return arrow::Status::Invalid(
          fmt::format("Unexpected \"to\" TimeUnit type: {}", to));
  }
}

inline const std::unordered_map<arrow::TimeUnit::type, time_utils::TimeUnit>
    ARROW_TIME_UNIT_MAPPING{{arrow::TimeUnit::NANO, time_utils::NANO},
                            {arrow::TimeUnit::MICRO, time_utils::MICRO},
                            {arrow::TimeUnit::MILLI, time_utils::MILLI},
                            {arrow::TimeUnit::SECOND, time_utils::SECOND}};

}  // namespace

arrow::Result<int64_t> convertTime(int64_t time, TimeUnit from, TimeUnit to) {
  if (from == to) {
    return time;
  }

  switch (from) {
    case NANO: {
      std::chrono::nanoseconds from_duration{time};
      return convertToTimeUnit(from_duration, to);
    }
    case MICRO: {
      std::chrono::microseconds from_duration{time};
      return convertToTimeUnit(from_duration, to);
    }
    case MILLI: {
      std::chrono::milliseconds from_duration{time};
      return convertToTimeUnit(from_duration, to);
    }
    case SECOND: {
      std::chrono::seconds from_duration{time};
      return convertToTimeUnit(from_duration, to);
    }
    case MINUTE: {
      std::chrono::minutes from_duration{time};
      return convertToTimeUnit(from_duration, to);
    }
    case HOUR: {
      std::chrono::hours from_duration{time};
      return convertToTimeUnit(from_duration, to);
    }
    case DAY: {
      std::chrono::duration<int64_t, std::ratio<SECONDS_IN_DAY>>
          from_duration{time};

      return convertToTimeUnit(from_duration, to);
    }
    case WEEK: {
      std::chrono::duration<int64_t, std::ratio<SECONDS_IN_WEEK>>
          from_duration{time};

      return convertToTimeUnit(from_duration, to);
    }
    default:
      return arrow::Status::Invalid(
          fmt::format("Unexpected \"from\" TimeUnit type: {}", from));
  }
}

TimeUnit mapArrowTimeUnit(arrow::TimeUnit::type arrow_time_unit) {
  return ARROW_TIME_UNIT_MAPPING.at(arrow_time_unit);
}

}  // namespace time_utils
}  // namespace stream_data_processor
