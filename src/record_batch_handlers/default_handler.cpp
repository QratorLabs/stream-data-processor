#include "default_handler.h"

#include "utils/serializer.h"

template <>
arrow::Status DefaultHandler::addMissingColumn<int64_t>(
    const std::unordered_map<std::string, int64_t>& missing_columns,
    arrow::RecordBatchVector* record_batches) const {
  for (auto& [column_name, default_value] : missing_columns) {
    if (record_batches->back()->schema()->GetFieldByName(column_name) !=
        nullptr) {
      continue;
    }

    for (auto& record_batch : *record_batches) {
      std::vector column_values(record_batch->num_rows(), default_value);
      arrow::Int64Builder builder;
      ARROW_RETURN_NOT_OK(builder.AppendValues(column_values));
      std::shared_ptr<arrow::Array> array;
      ARROW_RETURN_NOT_OK(builder.Finish(&array));
      auto add_column_result = record_batch->AddColumn(
          record_batch->num_columns(),
          arrow::field(column_name, arrow::int64()), array);
      if (!add_column_result.ok()) {
        return add_column_result.status();
      }

      record_batch = add_column_result.ValueOrDie();
    }
  }

  return arrow::Status::OK();
}

template <>
arrow::Status DefaultHandler::addMissingColumn<double>(
    const std::unordered_map<std::string, double>& missing_columns,
    arrow::RecordBatchVector* record_batches) const {
  for (auto& [column_name, default_value] : missing_columns) {
    if (record_batches->back()->schema()->GetFieldByName(column_name) !=
        nullptr) {
      continue;
    }

    for (auto& record_batch : *record_batches) {
      std::vector column_values(record_batch->num_rows(), default_value);
      arrow::DoubleBuilder builder;
      ARROW_RETURN_NOT_OK(builder.AppendValues(column_values));
      std::shared_ptr<arrow::Array> array;
      ARROW_RETURN_NOT_OK(builder.Finish(&array));
      auto add_column_result = record_batch->AddColumn(
          record_batch->num_columns(),
          arrow::field(column_name, arrow::float64()), array);
      if (!add_column_result.ok()) {
        return add_column_result.status();
      }

      record_batch = add_column_result.ValueOrDie();
    }
  }

  return arrow::Status::OK();
}

template <>
arrow::Status DefaultHandler::addMissingColumn<std::string>(
    const std::unordered_map<std::string, std::string>& missing_columns,
    arrow::RecordBatchVector* record_batches) const {
  for (auto& [column_name, default_value] : missing_columns) {
    if (record_batches->back()->schema()->GetFieldByName(column_name) !=
        nullptr) {
      continue;
    }

    for (auto& record_batch : *record_batches) {
      std::vector column_values(record_batch->num_rows(), default_value);
      arrow::StringBuilder builder;
      ARROW_RETURN_NOT_OK(builder.AppendValues(column_values));
      std::shared_ptr<arrow::Array> array;
      ARROW_RETURN_NOT_OK(builder.Finish(&array));
      auto add_column_result = record_batch->AddColumn(
          record_batch->num_columns(),
          arrow::field(column_name, arrow::utf8()), array);
      if (!add_column_result.ok()) {
        return add_column_result.status();
      }

      record_batch = add_column_result.ValueOrDie();
    }
  }

  return arrow::Status::OK();
}

template <>
arrow::Status DefaultHandler::addMissingColumn<bool>(
    const std::unordered_map<std::string, bool>& missing_columns,
    arrow::RecordBatchVector* record_batches) const {
  for (auto& [column_name, default_value] : missing_columns) {
    if (record_batches->back()->schema()->GetFieldByName(column_name) !=
        nullptr) {
      continue;
    }

    for (auto& record_batch : *record_batches) {
      std::vector column_values(record_batch->num_rows(), default_value);
      arrow::BooleanBuilder builder;
      ARROW_RETURN_NOT_OK(builder.AppendValues(column_values));
      std::shared_ptr<arrow::Array> array;
      ARROW_RETURN_NOT_OK(builder.Finish(&array));
      auto add_column_result = record_batch->AddColumn(
          record_batch->num_columns(),
          arrow::field(column_name, arrow::boolean()), array);
      if (!add_column_result.ok()) {
        return add_column_result.status();
      }

      record_batch = add_column_result.ValueOrDie();
    }
  }

  return arrow::Status::OK();
}

arrow::Status DefaultHandler::handle(
    const arrow::RecordBatchVector& record_batches,
    arrow::RecordBatchVector* result) {
  for (auto& record_batch : record_batches) {
    result->push_back(arrow::RecordBatch::Make(record_batch->schema(),
                                               record_batch->num_rows(),
                                               record_batch->columns()));
    if (record_batch->schema()->HasMetadata()) {
      result->back() = result->back()->ReplaceSchemaMetadata(
          record_batch->schema()->metadata());
    }
  }

  ARROW_RETURN_NOT_OK(
      addMissingColumn(options_.int64_columns_default_values, result));
  ARROW_RETURN_NOT_OK(
      addMissingColumn(options_.double_columns_default_values, result));
  ARROW_RETURN_NOT_OK(
      addMissingColumn(options_.string_columns_default_values, result));
  ARROW_RETURN_NOT_OK(
      addMissingColumn(options_.bool_columns_default_values, result));

  return arrow::Status::OK();
}
