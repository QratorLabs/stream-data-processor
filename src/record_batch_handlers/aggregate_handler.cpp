#include <unordered_set>
#include <utility>

#include "aggregate_handler.h"

#include "aggregate_functions/aggregate_functions.h"

#include "utils/utils.h"

const std::unordered_map<AggregateHandler::AggregateFunctionEnumType,
                         std::shared_ptr<AggregateFunction> >
    AggregateHandler::TYPES_TO_FUNCTIONS{
        {kFirst, std::make_shared<FirstAggregateFunction>()},
        {kLast, std::make_shared<LastAggregateFunction>()},
        {kMax, std::make_shared<MaxAggregateFunction>()},
        {kMin, std::make_shared<MinAggregateFunction>()},
        {kMean, std::make_shared<MeanAggregateFunction>()}};

AggregateHandler::AggregateHandler(
    const AggregateHandler::AggregateOptions& options)
    : options_(options) {}

AggregateHandler::AggregateHandler(
    AggregateHandler::AggregateOptions&& options)
    : options_(std::move(options)) {}

arrow::Status AggregateHandler::handle(
    const arrow::RecordBatchVector& record_batches,
    arrow::RecordBatchVector* result) {
  // Concatenating record batches to the one batch
  std::shared_ptr<arrow::RecordBatch> record_batch;
  ARROW_RETURN_NOT_OK(
      DataConverter::concatenateRecordBatches(record_batches, &record_batch));

  // Trying to find timestamp field for time window or first/last aggregators
  if (options_.add_result_time_column && options_.time_column_name.empty()) {
    return arrow::Status::Invalid(
        "Timestamp column is necessary for adding result time column");
  }

  // Splitting into groups in sorted order
  arrow::RecordBatchVector groups;
  ARROW_RETURN_NOT_OK(ComputeUtils::groupSortingByColumns(
      options_.grouping_columns, record_batch, &groups));

  // Setting result schema
  if (result_schema_ == nullptr) {
    ARROW_RETURN_NOT_OK(fillResultSchema(record_batch));
  }

  auto pool = arrow::default_memory_pool();
  arrow::ArrayVector result_arrays;

  // Calculating "first" and "last" timestamps if needed
  if (options_.add_result_time_column) {
    ARROW_RETURN_NOT_OK(
        aggregateTimeColumn(groups, kFirst, &result_arrays, pool));
  }

  // Adding unique group values of grouping columns to the result vector
  ARROW_RETURN_NOT_OK(fillGroupingColumns(groups, &result_arrays));

  // Calculating aggregators
  for (auto& [column_name, aggregate_functions] :
       options_.aggregate_columns) {
    for (auto& aggregate_case : aggregate_functions) {
      ARROW_RETURN_NOT_OK(aggregate(groups, column_name,
                                    aggregate_case.aggregate_function,
                                    &result_arrays, pool));
    }
  }

  result->push_back(
      arrow::RecordBatch::Make(result_schema_, groups.size(), result_arrays));
  return arrow::Status::OK();
}

arrow::Status AggregateHandler::fillResultSchema(
    const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  arrow::FieldVector result_fields;
  if (options_.add_result_time_column) {
    auto ts_column_type =
        record_batch->GetColumnByName(options_.time_column_name)->type();
    result_fields.push_back(
        arrow::field(options_.result_time_column_rule.result_column_name,
                     arrow::timestamp(arrow::TimeUnit::SECOND)));
  }

  for (auto& grouping_column_name : options_.grouping_columns) {
    auto column = record_batch->GetColumnByName(grouping_column_name);
    if (column != nullptr) {
      result_fields.push_back(
          arrow::field(grouping_column_name, column->type()));
    }
  }

  for (auto& [column_name, aggregate_cases] : options_.aggregate_columns) {
    auto column = record_batch->GetColumnByName(column_name);
    std::shared_ptr<arrow::DataType> column_type;
    if (column == nullptr) {
      column_type = arrow::null();
    } else {
      column_type = column->type();
    }

    for (auto& aggregate_case : aggregate_cases) {
      result_fields.push_back(
          arrow::field(aggregate_case.result_column_name, column_type));
    }
  }

  result_schema_ = arrow::schema(result_fields);
  return arrow::Status::OK();
}

arrow::Status AggregateHandler::aggregate(
    const arrow::RecordBatchVector& groups,
    const std::string& aggregate_column_name,
    const AggregateFunctionEnumType& aggregate_function,
    arrow::ArrayVector* result_arrays, arrow::MemoryPool* pool) const {
  auto aggregate_column_field =
      groups.front()->schema()->GetFieldByName(aggregate_column_name);
  if (aggregate_column_field == nullptr) {
    arrow::NullBuilder null_builder;
    ARROW_RETURN_NOT_OK(null_builder.AppendNulls(groups.size()));
    result_arrays->emplace_back();
    ARROW_RETURN_NOT_OK(null_builder.Finish(&result_arrays->back()));
    return arrow::Status::OK();
  }

  std::shared_ptr<arrow::ArrayBuilder> builder;
  ARROW_RETURN_NOT_OK(ArrowUtils::makeArrayBuilder(
      aggregate_column_field->type()->id(), &builder, pool));
  for (auto& group : groups) {
    std::shared_ptr<arrow::Scalar> aggregated_value;
    ARROW_RETURN_NOT_OK(TYPES_TO_FUNCTIONS.at(aggregate_function)
                            ->aggregate(group, aggregate_column_name,
                                        &aggregated_value,
                                        options_.time_column_name));
    ARROW_RETURN_NOT_OK(ArrowUtils::appendToBuilder(
        aggregated_value, &builder, aggregate_column_field->type()->id()));
  }

  result_arrays->emplace_back();
  ARROW_RETURN_NOT_OK(builder->Finish(&result_arrays->back()));
  return arrow::Status::OK();
}

arrow::Status AggregateHandler::aggregateTimeColumn(
    const arrow::RecordBatchVector& groups,
    const AggregateFunctionEnumType& aggregate_function,
    arrow::ArrayVector* result_arrays, arrow::MemoryPool* pool) const {
  auto ts_column_type =
      groups.front()->GetColumnByName(options_.time_column_name)->type();
  if (!ts_column_type->Equals(arrow::timestamp(arrow::TimeUnit::SECOND))) {
    return arrow::Status::NotImplemented(
        "Aggregation currently supports arrow::timestamp(SECOND) type for "
        "timestamp "
        "field only");  // TODO: support any numeric type
  }

  arrow::NumericBuilder<arrow::TimestampType> ts_builder(ts_column_type,
                                                         pool);
  for (auto& group : groups) {
    std::shared_ptr<arrow::Scalar> ts;
    ARROW_RETURN_NOT_OK(TYPES_TO_FUNCTIONS.at(aggregate_function)
                            ->aggregate(group, options_.time_column_name, &ts,
                                        options_.time_column_name));
    auto ts_cast_result = ts->CastTo(ts_column_type);
    if (!ts_cast_result.ok()) {
      return ts_cast_result.status();
    }

    ARROW_RETURN_NOT_OK(
        ts_builder.Append(std::static_pointer_cast<arrow::TimestampScalar>(
                              ts_cast_result.ValueOrDie())
                              ->value));
  }

  result_arrays->push_back(std::shared_ptr<arrow::TimestampArray>());
  ARROW_RETURN_NOT_OK(ts_builder.Finish(&result_arrays->back()));
  return arrow::Status::OK();
}

arrow::Status AggregateHandler::fillGroupingColumns(
    const arrow::RecordBatchVector& groups,
    arrow::ArrayVector* result_arrays) const {
  std::unordered_set<std::string> grouping_columns;
  for (auto& grouping_column : options_.grouping_columns) {
    grouping_columns.insert(grouping_column);
  }

  arrow::RecordBatchVector cropped_groups;
  for (auto& group : groups) {
    std::shared_ptr<arrow::RecordBatch> cropped_record_batch = group;
    int i = 0;
    for (auto& column_name : group->schema()->field_names()) {
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
  ARROW_RETURN_NOT_OK(DataConverter::concatenateRecordBatches(
      cropped_groups, &unique_record_batch));

  for (auto& grouping_column : unique_record_batch->columns()) {
    result_arrays->push_back(grouping_column);
  }

  return arrow::Status();
}
