#include <string>
#include <unordered_map>
#include <unordered_set>

#include <spdlog/spdlog.h>

#include "help.h"
#include "time_metadata.h"

namespace stream_data_processor {
namespace metadata {

namespace {

inline const std::string TIME_UNIT_METADATA_KEY{"time_unit"};

inline const std::unordered_map<time_utils::TimeUnit, std::string>
    TIME_UNITS_TO_NAMES{
        {time_utils::NANO, "NANO"},     {time_utils::MICRO, "MICRO"},
        {time_utils::MILLI, "MILLI"},   {time_utils::SECOND, "SECOND"},
        {time_utils::MINUTE, "MINUTE"}, {time_utils::HOUR, "HOUR"},
        {time_utils::DAY, "DAY"},       {time_utils::WEEK, "WEEK"}};

inline const std::unordered_map<std::string, time_utils::TimeUnit>
    TIME_NAMES_TO_UNITS{
        {"NANO", time_utils::NANO},     {"MICRO", time_utils::MICRO},
        {"MILLI", time_utils::MILLI},   {"SECOND", time_utils::SECOND},
        {"MINUTE", time_utils::MINUTE}, {"HOUR", time_utils::HOUR},
        {"DAY", time_utils::DAY},       {"WEEK", time_utils::WEEK}};

inline const std::unordered_set<arrow::Type::type>
    TIME_COMPATIBLE_ARROW_TYPES{
        arrow::Type::DURATION, arrow::Type::INT16,  arrow::Type::INT32,
        arrow::Type::INT64,    arrow::Type::INT8,   arrow::Type::UINT16,
        arrow::Type::UINT32,   arrow::Type::UINT64, arrow::Type::UINT8,
        arrow::Type::TIMESTAMP};

}  // namespace

arrow::Status setTimeUnitMetadata(std::shared_ptr<arrow::Field>* field,
                                  time_utils::TimeUnit time_unit) {
  auto arrow_type = field->get()->type();
  bool is_arrow_time_type = true;
  arrow::TimeUnit::type arrow_duration_time_unit;
  switch (arrow_type->id()) {
    case arrow::Type::DURATION:
      arrow_duration_time_unit =
          std::static_pointer_cast<arrow::DurationType>(arrow_type)->unit();
      break;
    case arrow::Type::TIMESTAMP:
      arrow_duration_time_unit =
          std::static_pointer_cast<arrow::TimestampType>(arrow_type)->unit();
      break;
    default: is_arrow_time_type = false;
  }

  if (is_arrow_time_type &&
      time_unit != time_utils::mapArrowTimeUnit(arrow_duration_time_unit)) {
    return arrow::Status::TypeError(
        fmt::format("Can't set {} time unit metadata to field with arrow "
                    "duration {} type",
                    time_unit, arrow_duration_time_unit));
  }

  if (TIME_COMPATIBLE_ARROW_TYPES.find(arrow_type->id()) ==
      TIME_COMPATIBLE_ARROW_TYPES.end()) {
    return arrow::Status::TypeError(fmt::format(
        "{} arrow type is not duration-compatible", arrow_type->id()));
  }

  ARROW_RETURN_NOT_OK(help::setFieldMetadata(
      field, TIME_UNIT_METADATA_KEY, TIME_UNITS_TO_NAMES.at(time_unit)));

  return arrow::Status::OK();
}

arrow::Status setTimeUnitMetadata(
    std::shared_ptr<arrow::RecordBatch>* record_batch,
    const std::string& column_name, time_utils::TimeUnit time_unit) {
  auto field = record_batch->get()->schema()->GetFieldByName(column_name);
  if (field == nullptr) {
    return arrow::Status::KeyError(fmt::format(
        "No such column or the choice is ambiguous: {}", column_name));
  }

  ARROW_RETURN_NOT_OK(setTimeUnitMetadata(&field, time_unit));
  ARROW_RETURN_NOT_OK(help::replaceField(record_batch, field));
  return arrow::Status::OK();
}

arrow::Result<time_utils::TimeUnit> getTimeUnitMetadata(
    const arrow::Field& field) {
  std::string time_unit_name;
  ARROW_ASSIGN_OR_RAISE(
      time_unit_name, help::getFieldMetadata(field, TIME_UNIT_METADATA_KEY));
  if (TIME_NAMES_TO_UNITS.find(time_unit_name) == TIME_NAMES_TO_UNITS.end()) {
    return arrow::Status::Invalid(
        fmt::format("Unexpected time unit name: {}", time_unit_name));
  }

  return TIME_NAMES_TO_UNITS.at(time_unit_name);
}

arrow::Result<time_utils::TimeUnit> getTimeUnitMetadata(
    const arrow::RecordBatch& record_batch, const std::string& column_name) {
  auto field = record_batch.schema()->GetFieldByName(column_name);
  if (field == nullptr) {
    return arrow::Status::KeyError(
        fmt::format("RecordBatch has no such column or it is not unique: {}",
                    column_name));
  }

  return getTimeUnitMetadata(*field);
}

}  // namespace metadata
}  // namespace stream_data_processor