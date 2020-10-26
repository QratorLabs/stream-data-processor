#include "default_handler.h"

#include "utils/serializer.h"

template <>
arrow::Status DefaultHandler::addMissingColumn<int64_t>(
    const std::unordered_map<std::string, int64_t>& missing_columns,
    std::shared_ptr<arrow::RecordBatch>* record_batch) const {
  for (auto& [column_name, default_value] : missing_columns) {
    if (record_batch->get()->schema()->GetFieldByName(column_name) !=
        nullptr) {
      continue;
    }

    std::vector column_values(record_batch->get()->num_rows(), default_value);
    arrow::Int64Builder builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues(column_values));
    std::shared_ptr<arrow::Array> array;
    ARROW_RETURN_NOT_OK(builder.Finish(&array));
    auto add_column_result = record_batch->get()->AddColumn(
        record_batch->get()->num_columns(),
        arrow::field(column_name, arrow::int64()), array);
    if (!add_column_result.ok()) {
      return add_column_result.status();
    }

    *record_batch = add_column_result.ValueOrDie();
  }

  return arrow::Status::OK();
}

template <>
arrow::Status DefaultHandler::addMissingColumn<double>(
    const std::unordered_map<std::string, double>& missing_columns,
    std::shared_ptr<arrow::RecordBatch>* record_batch) const {
  for (auto& [column_name, default_value] : missing_columns) {
    if (record_batch->get()->schema()->GetFieldByName(column_name) !=
        nullptr) {
      continue;
    }

    std::vector column_values(record_batch->get()->num_rows(), default_value);
    arrow::DoubleBuilder builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues(column_values));
    std::shared_ptr<arrow::Array> array;
    ARROW_RETURN_NOT_OK(builder.Finish(&array));
    auto add_column_result = record_batch->get()->AddColumn(
        record_batch->get()->num_columns(),
        arrow::field(column_name, arrow::float64()), array);
    if (!add_column_result.ok()) {
      return add_column_result.status();
    }

    *record_batch = add_column_result.ValueOrDie();
  }

  return arrow::Status::OK();
}

template <>
arrow::Status DefaultHandler::addMissingColumn<std::string>(
    const std::unordered_map<std::string, std::string>& missing_columns,
    std::shared_ptr<arrow::RecordBatch>* record_batch) const {
  for (auto& [column_name, default_value] : missing_columns) {
    if (record_batch->get()->schema()->GetFieldByName(column_name) !=
        nullptr) {
      continue;
    }

    std::vector column_values(record_batch->get()->num_rows(), default_value);
    arrow::StringBuilder builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues(column_values));
    std::shared_ptr<arrow::Array> array;
    ARROW_RETURN_NOT_OK(builder.Finish(&array));
    auto add_column_result = record_batch->get()->AddColumn(
        record_batch->get()->num_columns(),
        arrow::field(column_name, arrow::utf8()), array);
    if (!add_column_result.ok()) {
      return add_column_result.status();
    }

    *record_batch = add_column_result.ValueOrDie();
  }

  return arrow::Status::OK();
}

template <>
arrow::Status DefaultHandler::addMissingColumn<bool>(
    const std::unordered_map<std::string, bool>& missing_columns,
    std::shared_ptr<arrow::RecordBatch>* record_batch) const {
  for (auto& [column_name, default_value] : missing_columns) {
    if (record_batch->get()->schema()->GetFieldByName(column_name) !=
        nullptr) {
      continue;
    }

    std::vector column_values(record_batch->get()->num_rows(), default_value);
    arrow::BooleanBuilder builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues(column_values));
    std::shared_ptr<arrow::Array> array;
    ARROW_RETURN_NOT_OK(builder.Finish(&array));
    auto add_column_result = record_batch->get()->AddColumn(
        record_batch->get()->num_columns(),
        arrow::field(column_name, arrow::boolean()), array);
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
      addMissingColumn(options_.int64_columns_default_values, &copy_record_batch));
  ARROW_RETURN_NOT_OK(
      addMissingColumn(options_.double_columns_default_values, &copy_record_batch));
  ARROW_RETURN_NOT_OK(
      addMissingColumn(options_.string_columns_default_values, &copy_record_batch));
  ARROW_RETURN_NOT_OK(
      addMissingColumn(options_.bool_columns_default_values, &copy_record_batch));

  copyMetadata(record_batch, &copy_record_batch);
  result->push_back(copy_record_batch);

  return arrow::Status::OK();
}
