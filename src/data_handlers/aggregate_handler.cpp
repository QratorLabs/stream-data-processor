#include <unordered_set>

#include "aggregate_handler.h"

#include "data_handlers/aggregate_functions/aggregate_functions.h"

#include "utils/utils.h"

const std::unordered_map<std::string, std::shared_ptr<AggregateFunction>> AggregateHandler::NAMES_TO_FUNCTIONS{
    {"first", std::make_shared<FirstAggregateFunction>()},
    {"last", std::make_shared<LastAggregateFunction>()},
    {"max", std::make_shared<MaxAggregateFunction>()},
    {"min", std::make_shared<MinAggregateFunction>()}
};

AggregateHandler::AggregateHandler(std::vector<std::string> grouping_columns,
                                   AggregateOptions options,
                                   std::string ts_column_name)
                                   : grouping_columns_(std::move(grouping_columns))
                                   , options_(std::move(options))
                                   , ts_column_name_(std::move(ts_column_name)) {

}

arrow::Status AggregateHandler::handle(std::shared_ptr<arrow::Buffer> source, std::shared_ptr<arrow::Buffer> *target) {
  // Converting arrow::Buffer to arrow::RecordBatchVector
  std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;
  ARROW_RETURN_NOT_OK(Serializer::deserializeRecordBatches(source, &record_batches));
  if (record_batches.empty()) {
    return arrow::Status::CapacityError("No data to aggregate");
  }

  // Concatenating record batches to the one batch
  std::shared_ptr<arrow::RecordBatch> record_batch;
  ARROW_RETURN_NOT_OK(DataConverter::concatenateRecordBatches(record_batches, &record_batch));

  // Trying to find timestamp field for time window or first/last aggregators
  ARROW_RETURN_NOT_OK(findTsColumnName(record_batch));
  if (options_.add_time_window_columns_ && ts_column_name_.empty()) {
    return arrow::Status::Invalid("Timestamp column is necessary for adding time window columns");
  }

  // Splitting into groups in sorted order
  arrow::RecordBatchVector groups;
  ARROW_RETURN_NOT_OK(ComputeUtils::groupSortingByColumns(grouping_columns_, record_batch, groups));

  // Setting result schema
  if (result_schema_ == nullptr) {
    ARROW_RETURN_NOT_OK(fillResultSchema(record_batch));
  }

  auto pool = arrow::default_memory_pool();
  arrow::ArrayVector result_arrays;

  // Calculating "first" and "last" timestamps if needed
  if (options_.add_time_window_columns_) {
    ARROW_RETURN_NOT_OK(aggregateTsColumn(groups, "first", result_arrays, pool));
    ARROW_RETURN_NOT_OK(aggregateTsColumn(groups, "last", result_arrays, pool));
  }

  // Adding unique group values of grouping columns to the result vector
  ARROW_RETURN_NOT_OK(fillGroupingColumns(groups, result_arrays));

  // Calculating aggregators
  for (auto &aggregating_column : options_.aggregate_columns) {
    for (auto &aggregation_function : aggregating_column.second) {
      ARROW_RETURN_NOT_OK(aggregate(groups, aggregating_column.first, aggregation_function, result_arrays, pool));
    }
  }

  auto result_record_batch = arrow::RecordBatch::Make(result_schema_, groups.size(), result_arrays);

  ARROW_RETURN_NOT_OK(Serializer::serializeRecordBatches(result_schema_, { result_record_batch }, target));
  return arrow::Status::OK();
}

arrow::Status AggregateHandler::findTsColumnName(const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  if (!ts_column_name_.empty()) {
    auto ts_column = record_batch->schema()->GetFieldByName(ts_column_name_);
    if (ts_column == nullptr) {
      return arrow::Status::Invalid("Wrong timestamp column name: " + ts_column_name_);
    }

    return arrow::Status::OK();
  }

  for (auto& field : record_batch->schema()->fields()) {
    if (field->type()->id() == arrow::Type::TIMESTAMP) {
      if (ts_column_name_.empty()) {
        ts_column_name_ = field->name();
      } else {
        return arrow::Status::Invalid("Timestamp column detection is ambiguous");
      }
    }
  }

  return arrow::Status::OK();
}

arrow::Status AggregateHandler::fillResultSchema(const std::shared_ptr<arrow::RecordBatch> &record_batch) {
  arrow::FieldVector result_fields;
  if (options_.add_time_window_columns_) {
    auto ts_column_type = record_batch->GetColumnByName(ts_column_name_)->type();
    result_fields.push_back(arrow::field("from_ts", ts_column_type));
    result_fields.push_back(arrow::field("to_ts", ts_column_type));
  }

  for (auto& grouping_column_name : grouping_columns_) {
    auto column = record_batch->GetColumnByName(grouping_column_name);
    if (column == nullptr) {
      return arrow::Status::Invalid("RecordBatch doesn't have such column: " + grouping_column_name);
    }

    result_fields.push_back(arrow::field(grouping_column_name, column->type()));
  }

  for (auto& aggregate_column : options_.aggregate_columns) {
    auto column = record_batch->GetColumnByName(aggregate_column.first);
    if (column == nullptr) {
      return arrow::Status::Invalid("RecordBatch doesn't have such column: " + aggregate_column.first);
    }

    for (auto& aggregate_function : aggregate_column.second) {
      result_fields.push_back(arrow::field(aggregate_column.first + '_' + aggregate_function, column->type()));
    }
  }

  result_schema_ = arrow::schema(result_fields);
  return arrow::Status::OK();
}

arrow::Status AggregateHandler::aggregate(const arrow::RecordBatchVector &groups,
                                          const std::string &aggregate_column_name,
                                          const std::string &aggregate_function,
                                          arrow::ArrayVector &result_arrays,
                                          arrow::MemoryPool *pool) const {
  auto aggregate_column_field = groups.front()->schema()->GetFieldByName(aggregate_column_name);
  if (!aggregate_column_field->type()->Equals(arrow::int64())) {
    return arrow::Status::NotImplemented("Aggregation currently support arrow::int64 type fields only"); // TODO: support any type
  }

  arrow::Int64Builder builder(pool);
  for (auto& group : groups) {
    std::shared_ptr<arrow::Scalar> aggregated_value;
    ARROW_RETURN_NOT_OK(NAMES_TO_FUNCTIONS.at(aggregate_function)->aggregate(group,
                                                                             aggregate_column_name,
                                                                             &aggregated_value,
                                                                             ts_column_name_));
    auto value_cast_result = aggregated_value->CastTo(arrow::int64());
    if (!value_cast_result.ok()) {
      return value_cast_result.status();
    }

    ARROW_RETURN_NOT_OK(builder.Append(std::static_pointer_cast<arrow::Int64Scalar>(value_cast_result.ValueOrDie())->value));
  }

  result_arrays.push_back(std::shared_ptr<arrow::Int64Array>());
  ARROW_RETURN_NOT_OK(builder.Finish(&result_arrays.back()));
  return arrow::Status::OK();
}

arrow::Status AggregateHandler::aggregateTsColumn(const arrow::RecordBatchVector &groups,
                                            const std::string &aggregate_function,
                                            arrow::ArrayVector &result_arrays,
                                            arrow::MemoryPool *pool) const {
  auto ts_column_type = groups.front()->GetColumnByName(ts_column_name_)->type();
  if (!ts_column_type->Equals(arrow::timestamp(arrow::TimeUnit::SECOND))) {
    return arrow::Status::NotImplemented("Aggregation currently supports arrow::timestamp type for timestamp field only"); // TODO: support any numeric type
  }

  arrow::NumericBuilder<arrow::TimestampType> ts_builder(ts_column_type, pool);
  for (auto& group : groups) {
    std::shared_ptr<arrow::Scalar> ts;
    ARROW_RETURN_NOT_OK(NAMES_TO_FUNCTIONS.at(aggregate_function)->aggregate(group,
                                                                             ts_column_name_,
                                                                             &ts,
                                                                             ts_column_name_));
    auto ts_cast_result = ts->CastTo(ts_column_type);
    if (!ts_cast_result.ok()) {
      return ts_cast_result.status();
    }

    ARROW_RETURN_NOT_OK(ts_builder.Append(std::static_pointer_cast<arrow::TimestampScalar>(ts_cast_result.ValueOrDie())->value));
  }

  result_arrays.push_back(std::shared_ptr<arrow::TimestampArray>());
  ARROW_RETURN_NOT_OK(ts_builder.Finish(&result_arrays.back()));
  return arrow::Status::OK();
}

arrow::Status AggregateHandler::fillGroupingColumns(const arrow::RecordBatchVector& groups,
                                                    arrow::ArrayVector &result_arrays) const {
  std::unordered_set<std::string> grouping_columns;
  for (auto& grouping_column : grouping_columns_) {
    grouping_columns.insert(grouping_column);
  }

  arrow::RecordBatchVector cropped_groups;
  for (auto& group : groups) {
    std::shared_ptr<arrow::RecordBatch> cropped_record_batch = group;
    int i = 0;
    for (auto &column_name : group->schema()->field_names()) {
      if (grouping_columns.find(column_name) == grouping_columns.end()) {
        auto cropped_result = cropped_record_batch->RemoveColumn(i);
        if (!cropped_result.ok()) {
          return cropped_result.status();
        }

        cropped_record_batch = cropped_result.ValueOrDie();
        --i;
      }

      ++i;
    }

    cropped_groups.push_back(cropped_record_batch->Slice(0, 1));
  }

  std::shared_ptr<arrow::RecordBatch> unique_record_batch;
  ARROW_RETURN_NOT_OK(DataConverter::concatenateRecordBatches(cropped_groups, &unique_record_batch));

  for (auto& grouping_column : unique_record_batch->columns()) {
    result_arrays.push_back(grouping_column);
  }

  return arrow::Status();
}
