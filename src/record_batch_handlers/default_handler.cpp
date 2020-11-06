#include "default_handler.h"

#include "metadata/column_typing.h"
#include "utils/serialize_utils.h"

namespace stream_data_processor {

template <>
arrow::Status DefaultHandler::addMissingColumn<int64_t>(
    const std::unordered_map<std::string, DefaultCase<int64_t>>&
        default_cases,
    std::shared_ptr<arrow::RecordBatch>* record_batch) const {
  for (auto& [column_name, default_case] : default_cases) {
    if (record_batch->get()->schema()->GetFieldByName(column_name) !=
        nullptr) {
      continue;
    }

    std::vector column_values(record_batch->get()->num_rows(),
                              default_case.default_value);
    arrow::Int64Builder builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues(column_values));
    std::shared_ptr<arrow::Array> array;
    ARROW_RETURN_NOT_OK(builder.Finish(&array));

    auto new_field = arrow::field(column_name, arrow::int64());
    ARROW_RETURN_NOT_OK(metadata::setColumnTypeMetadata(
        &new_field, default_case.default_column_type));

    auto add_column_result = record_batch->get()->AddColumn(
        record_batch->get()->num_columns(), new_field, array);

    if (!add_column_result.ok()) {
      return add_column_result.status();
    }

    *record_batch = add_column_result.ValueOrDie();
  }

  return arrow::Status::OK();
}

template <>
arrow::Status DefaultHandler::addMissingColumn<double>(
    const std::unordered_map<std::string, DefaultCase<double>>& default_cases,
    std::shared_ptr<arrow::RecordBatch>* record_batch) const {
  for (auto& [column_name, default_case] : default_cases) {
    if (record_batch->get()->schema()->GetFieldByName(column_name) !=
        nullptr) {
      continue;
    }

    std::vector column_values(record_batch->get()->num_rows(),
                              default_case.default_value);
    arrow::DoubleBuilder builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues(column_values));
    std::shared_ptr<arrow::Array> array;
    ARROW_RETURN_NOT_OK(builder.Finish(&array));

    auto new_field = arrow::field(column_name, arrow::float64());
    ARROW_RETURN_NOT_OK(metadata::setColumnTypeMetadata(
        &new_field, default_case.default_column_type));

    auto add_column_result = record_batch->get()->AddColumn(
        record_batch->get()->num_columns(), new_field, array);
    if (!add_column_result.ok()) {
      return add_column_result.status();
    }

    *record_batch = add_column_result.ValueOrDie();
  }

  return arrow::Status::OK();
}

template <>
arrow::Status DefaultHandler::addMissingColumn<std::string>(
    const std::unordered_map<std::string, DefaultCase<std::string>>&
        default_cases,
    std::shared_ptr<arrow::RecordBatch>* record_batch) const {
  for (auto& [column_name, default_case] : default_cases) {
    if (record_batch->get()->schema()->GetFieldByName(column_name) !=
        nullptr) {
      continue;
    }

    std::vector column_values(record_batch->get()->num_rows(),
                              default_case.default_value);
    arrow::StringBuilder builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues(column_values));
    std::shared_ptr<arrow::Array> array;
    ARROW_RETURN_NOT_OK(builder.Finish(&array));

    auto new_field = arrow::field(column_name, arrow::utf8());
    ARROW_RETURN_NOT_OK(metadata::setColumnTypeMetadata(
        &new_field, default_case.default_column_type));

    auto add_column_result = record_batch->get()->AddColumn(
        record_batch->get()->num_columns(), new_field, array);
    if (!add_column_result.ok()) {
      return add_column_result.status();
    }

    *record_batch = add_column_result.ValueOrDie();
  }

  return arrow::Status::OK();
}

template <>
arrow::Status DefaultHandler::addMissingColumn<bool>(
    const std::unordered_map<std::string, DefaultCase<bool>>& default_cases,
    std::shared_ptr<arrow::RecordBatch>* record_batch) const {
  for (auto& [column_name, default_case] : default_cases) {
    if (record_batch->get()->schema()->GetFieldByName(column_name) !=
        nullptr) {
      continue;
    }

    std::vector column_values(record_batch->get()->num_rows(),
                              default_case.default_value);
    arrow::BooleanBuilder builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues(column_values));
    std::shared_ptr<arrow::Array> array;
    ARROW_RETURN_NOT_OK(builder.Finish(&array));

    auto new_field = arrow::field(column_name, arrow::boolean());
    ARROW_RETURN_NOT_OK(metadata::setColumnTypeMetadata(
        &new_field, default_case.default_column_type));

    auto add_column_result = record_batch->get()->AddColumn(
        record_batch->get()->num_columns(), new_field, array);
    if (!add_column_result.ok()) {
      return add_column_result.status();
    }

    *record_batch = add_column_result.ValueOrDie();
  }

  return arrow::Status::OK();
}

arrow::Status DefaultHandler::handle(
    const std::shared_ptr<arrow::RecordBatch>& record_batch,
    arrow::RecordBatchVector* result) {
  auto copy_record_batch = arrow::RecordBatch::Make(record_batch->schema(),
                                                    record_batch->num_rows(),
                                                    record_batch->columns());

  ARROW_RETURN_NOT_OK(
      addMissingColumn(options_.int64_default_cases, &copy_record_batch));
  ARROW_RETURN_NOT_OK(
      addMissingColumn(options_.double_default_cases, &copy_record_batch));
  ARROW_RETURN_NOT_OK(
      addMissingColumn(options_.string_default_cases, &copy_record_batch));
  ARROW_RETURN_NOT_OK(
      addMissingColumn(options_.bool_default_cases, &copy_record_batch));

  copySchemaMetadata(record_batch, &copy_record_batch);
  ARROW_RETURN_NOT_OK(copyColumnTypes(record_batch, &copy_record_batch));
  result->push_back(copy_record_batch);

  return arrow::Status::OK();
}

}  // namespace stream_data_processor
