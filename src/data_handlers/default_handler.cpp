#include "default_handler.h"

#include "utils/serializer.h"

template <>
arrow::Status DefaultHandler::addMissingColumn<int64_t>(const std::unordered_map<std::string, int64_t> &missing_columns,
                                                        arrow::RecordBatchVector &record_batches) const {
  for (auto& missing_column : missing_columns) {
    if (record_batches.back()->schema()->GetFieldByName(missing_column.first) != nullptr) {
      continue;
    }

    for (auto& record_batch : record_batches) {
      std::vector column_values(record_batch->num_rows(), missing_column.second);
      arrow::Int64Builder builder;
      ARROW_RETURN_NOT_OK(builder.AppendValues(column_values));
      std::shared_ptr<arrow::Array> array;
      ARROW_RETURN_NOT_OK(builder.Finish(&array));
      auto add_column_result = record_batch->AddColumn(record_batch->num_columns(),
                                                  arrow::field(missing_column.first, arrow::int64()),
                                                  array);
      if (!add_column_result.ok()) {
        return add_column_result.status();
      }

      record_batch = add_column_result.ValueOrDie();
    }
  }

  return arrow::Status::OK();
}

template <>
arrow::Status DefaultHandler::addMissingColumn<double>(const std::unordered_map<std::string, double> &missing_columns,
                                                       arrow::RecordBatchVector &record_batches) const {
  for (auto& missing_column : missing_columns) {
    if (record_batches.back()->schema()->GetFieldByName(missing_column.first) != nullptr) {
      continue;
    }

    for (auto& record_batch : record_batches) {
      std::vector column_values(record_batch->num_rows(), missing_column.second);
      arrow::DoubleBuilder builder;
      ARROW_RETURN_NOT_OK(builder.AppendValues(column_values));
      std::shared_ptr<arrow::Array> array;
      ARROW_RETURN_NOT_OK(builder.Finish(&array));
      auto add_column_result = record_batch->AddColumn(record_batch->num_columns(),
                                                       arrow::field(missing_column.first, arrow::float64()),
                                                       array);
      if (!add_column_result.ok()) {
        return add_column_result.status();
      }

      record_batch = add_column_result.ValueOrDie();
    }
  }

  return arrow::Status::OK();
}

template <>
arrow::Status DefaultHandler::addMissingColumn<std::string>(const std::unordered_map<std::string, std::string> &missing_columns,
                                                            arrow::RecordBatchVector &record_batches) const {
  for (auto& missing_column : missing_columns) {
    if (record_batches.back()->schema()->GetFieldByName(missing_column.first) != nullptr) {
      continue;
    }

    for (auto& record_batch : record_batches) {
      std::vector column_values(record_batch->num_rows(), missing_column.second);
      arrow::StringBuilder builder;
      ARROW_RETURN_NOT_OK(builder.AppendValues(column_values));
      std::shared_ptr<arrow::Array> array;
      ARROW_RETURN_NOT_OK(builder.Finish(&array));
      auto add_column_result = record_batch->AddColumn(record_batch->num_columns(),
                                                       arrow::field(missing_column.first, arrow::utf8()),
                                                       array);
      if (!add_column_result.ok()) {
        return add_column_result.status();
      }

      record_batch = add_column_result.ValueOrDie();
    }
  }

  return arrow::Status::OK();
}

template <>
arrow::Status DefaultHandler::addMissingColumn<bool>(const std::unordered_map<std::string, bool> &missing_columns,
                                                            arrow::RecordBatchVector &record_batches) const {
  for (auto& missing_column : missing_columns) {
    if (record_batches.back()->schema()->GetFieldByName(missing_column.first) != nullptr) {
      continue;
    }

    for (auto& record_batch : record_batches) {
      std::vector column_values(record_batch->num_rows(), missing_column.second);
      arrow::BooleanBuilder builder;
      ARROW_RETURN_NOT_OK(builder.AppendValues(column_values));
      std::shared_ptr<arrow::Array> array;
      ARROW_RETURN_NOT_OK(builder.Finish(&array));
      auto add_column_result = record_batch->AddColumn(record_batch->num_columns(),
                                                       arrow::field(missing_column.first, arrow::boolean()),
                                                       array);
      if (!add_column_result.ok()) {
        return add_column_result.status();
      }

      record_batch = add_column_result.ValueOrDie();
    }
  }

  return arrow::Status::OK();
}

arrow::Status DefaultHandler::handle(const std::shared_ptr<arrow::Buffer> &source,
                                     std::shared_ptr<arrow::Buffer> *target) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;
  ARROW_RETURN_NOT_OK(Serializer::deserializeRecordBatches(source, &record_batches));
  if (record_batches.empty()) {
    record_batches.push_back(arrow::RecordBatch::Make(arrow::schema({}), 0, arrow::ArrayVector()));
  }

  ARROW_RETURN_NOT_OK(addMissingColumn(options_.int64_columns_default_values, record_batches));
  ARROW_RETURN_NOT_OK(addMissingColumn(options_.double_columns_default_values, record_batches));
  ARROW_RETURN_NOT_OK(addMissingColumn(options_.string_columns_default_values, record_batches));
  ARROW_RETURN_NOT_OK(addMissingColumn(options_.bool_columns_default_values, record_batches));

  ARROW_RETURN_NOT_OK(Serializer::serializeRecordBatches(record_batches.back()->schema(),
                                                         record_batches, target));
  return arrow::Status::OK();
}
