#include <unordered_set>
#include <utility>

#include <spdlog/spdlog.h>

#include "aggregate_handler.h"

#include "aggregate_functions/aggregate_functions.h"
#include "metadata/column_typing.h"
#include "metadata/grouping.h"

#include "utils/utils.h"

namespace stream_data_processor {

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
    const std::shared_ptr<arrow::RecordBatch>& record_batch,
    arrow::RecordBatchVector* result) {
  if (record_batch->num_rows() == 0) {
    return arrow::Status::OK();
  }

  arrow::RecordBatchVector result_vector;
  ARROW_RETURN_NOT_OK(handle({record_batch}, &result_vector));
  if (result_vector.size() != 1) {
    return arrow::Status::ExecutionError(
        "Aggregation of one record batch "
        "should contain only one record");
  }

  result->push_back(result_vector.front());
  return arrow::Status::OK();
}

arrow::Status AggregateHandler::handle(
    const arrow::RecordBatchVector& record_batches,
    arrow::RecordBatchVector* result) {
  if (record_batches.empty()) {
    return arrow::Status::OK();
  }

  ARROW_RETURN_NOT_OK(isValid(record_batches));

  auto grouped = splitByGroups(record_batches);

  for ([[maybe_unused]] auto& [_, record_batches_group] : grouped) {
    if (record_batches_group.empty()) {
      return arrow::Status::ExecutionError(
          "RecordBatchVector after splitting"
          " by groups must be non-empty");
    }

    auto pool = arrow::default_memory_pool();
    arrow::ArrayVector result_arrays;

    auto grouping_columns =
        metadata::extractGroupingColumnsNames(record_batches_group.front());

    bool explicitly_add_measurement_field = true;
    bool has_measurement = false;
    std::string measurement_column_name;
    if (!metadata::getMeasurementColumnNameMetadata(record_batches.front(),
                                                    &measurement_column_name)
             .ok()) {
      explicitly_add_measurement_field = false;
    } else {
      has_measurement = true;
      for (auto& grouping_column : grouping_columns) {
        if (grouping_column == measurement_column_name) {
          explicitly_add_measurement_field = false;
        }
      }
    }

    std::shared_ptr<arrow::Schema> result_schema = nullptr;
    ARROW_RETURN_NOT_OK(fillResultSchema(
        record_batches, grouping_columns, &result_schema,
        explicitly_add_measurement_field, measurement_column_name));

    ARROW_RETURN_NOT_OK(
        aggregateTimeColumn(record_batches_group, &result_arrays, pool));

    if (explicitly_add_measurement_field) {
      ARROW_RETURN_NOT_OK(fillMeasurementColumn(
          record_batches_group, &result_arrays, measurement_column_name));
    }

    ARROW_RETURN_NOT_OK(fillGroupingColumns(
        record_batches_group, &result_arrays, grouping_columns));

    for (auto& [column_name, aggregate_functions] :
         options_.aggregate_columns) {
      for (auto& aggregate_case : aggregate_functions) {
        ARROW_RETURN_NOT_OK(aggregate(record_batches_group, column_name,
                                      aggregate_case.aggregate_function,
                                      &result_arrays, pool));
      }
    }

    result->push_back(arrow::RecordBatch::Make(
        result_schema, record_batches_group.size(), result_arrays));

    ARROW_RETURN_NOT_OK(
        metadata::fillGroupMetadata(&result->back(), grouping_columns));

    ARROW_RETURN_NOT_OK(metadata::setTimeColumnNameMetadata(
        &result->back(),
        options_.result_time_column_rule.result_column_name));

    if (has_measurement) {
      ARROW_RETURN_NOT_OK(metadata::setMeasurementColumnNameMetadata(
          &result->back(), measurement_column_name));
    }
  }

  return arrow::Status::OK();
}

arrow::Status AggregateHandler::fillResultSchema(
    const arrow::RecordBatchVector& record_batches,
    const std::vector<std::string>& grouping_columns,
    std::shared_ptr<arrow::Schema>* result_schema,
    bool explicitly_add_measurement,
    const std::string& measurement_column_name) const {
  arrow::FieldVector result_fields;
  result_fields.push_back(
      arrow::field(options_.result_time_column_rule.result_column_name,
                   arrow::timestamp(arrow::TimeUnit::SECOND)));

  ARROW_RETURN_NOT_OK(
      metadata::setColumnTypeMetadata(&result_fields.back(), metadata::TIME));

  if (explicitly_add_measurement) {
    result_fields.push_back(
        arrow::field(measurement_column_name, arrow::utf8()));

    ARROW_RETURN_NOT_OK(metadata::setColumnTypeMetadata(
        &result_fields.back(), metadata::MEASUREMENT));
  }

  for (auto& grouping_column_name : grouping_columns) {
    auto column_field = record_batches.front()->schema()->GetFieldByName(
        grouping_column_name);

    if (column_field != nullptr) {
      result_fields.push_back(
          arrow::field(grouping_column_name, column_field->type(),
                       column_field->nullable(), column_field->metadata()));
    }
  }

  for (auto& [column_name, aggregate_cases] : options_.aggregate_columns) {
    std::shared_ptr<arrow::DataType> column_type = arrow::null();
    for (auto& record_batch : record_batches) {
      auto column = record_batch->GetColumnByName(column_name);
      if (column != nullptr) {
        column_type = column->type();
        break;
      }
    }

    for (auto& aggregate_case : aggregate_cases) {
      result_fields.push_back(
          arrow::field(aggregate_case.result_column_name, column_type));

      ARROW_RETURN_NOT_OK(metadata::setColumnTypeMetadata(
          &result_fields.back(), aggregate_case.result_column_type));
    }
  }

  *result_schema = arrow::schema(result_fields);

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
  ARROW_RETURN_NOT_OK(arrow_utils::makeArrayBuilder(
      aggregate_column_field->type()->id(), &builder, pool));

  for (auto& group : groups) {
    std::shared_ptr<arrow::Scalar> aggregated_value;
    ARROW_RETURN_NOT_OK(
        TYPES_TO_FUNCTIONS.at(aggregate_function)
            ->aggregate(group, aggregate_column_name, &aggregated_value));

    ARROW_RETURN_NOT_OK(arrow_utils::appendToBuilder(
        aggregated_value, &builder, aggregate_column_field->type()->id()));
  }

  result_arrays->emplace_back();
  ARROW_RETURN_NOT_OK(builder->Finish(&result_arrays->back()));
  return arrow::Status::OK();
}

arrow::Status AggregateHandler::aggregateTimeColumn(
    const arrow::RecordBatchVector& record_batch_vector,
    arrow::ArrayVector* result_arrays, arrow::MemoryPool* pool) const {
  std::string time_column_name;
  ARROW_RETURN_NOT_OK(metadata::getTimeColumnNameMetadata(
      record_batch_vector.front(), &time_column_name));

  auto ts_column_type =
      record_batch_vector.front()->GetColumnByName(time_column_name)->type();

  arrow::NumericBuilder<arrow::TimestampType> ts_builder(ts_column_type,
                                                         pool);
  for (auto& group : record_batch_vector) {
    std::shared_ptr<arrow::Scalar> ts;

    ARROW_RETURN_NOT_OK(
        TYPES_TO_FUNCTIONS
            .at(options_.result_time_column_rule.aggregate_function)
            ->aggregate(group, time_column_name, &ts));

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
    const arrow::RecordBatchVector& grouped,
    arrow::ArrayVector* result_arrays,
    const std::vector<std::string>& grouping_columns) {
  std::unordered_set<std::string> grouping_columns_set;
  for (auto& grouping_column : grouping_columns) {
    grouping_columns_set.insert(grouping_column);
  }

  std::shared_ptr<arrow::RecordBatch> cropped_record_batch = grouped.front();
  int i = 0;
  for (auto& column_name : grouped.front()->schema()->field_names()) {
    if (grouping_columns_set.find(column_name) ==
        grouping_columns_set.end()) {
      auto cropped_result = cropped_record_batch->RemoveColumn(i);
      if (!cropped_result.ok()) {
        return cropped_result.status();
      }

      cropped_record_batch = cropped_result.ValueOrDie();
      --i;
    }

    ++i;
  }

  arrow::RecordBatchVector cropped_groups;
  cropped_record_batch = cropped_record_batch->Slice(0, 1);
  for (int j = 0; j < grouped.size(); ++j) {
    cropped_groups.push_back(arrow::RecordBatch::Make(
        cropped_record_batch->schema(), cropped_record_batch->num_rows(),
        cropped_record_batch->columns()));
  }

  std::shared_ptr<arrow::RecordBatch> unique_record_batch;
  ARROW_RETURN_NOT_OK(convert_utils::concatenateRecordBatches(
      cropped_groups, &unique_record_batch));

  for (auto& grouping_column : grouping_columns) {
    result_arrays->push_back(
        unique_record_batch->GetColumnByName(grouping_column));
  }

  return arrow::Status();
}

std::unordered_map<std::string, arrow::RecordBatchVector>
AggregateHandler::splitByGroups(
    const arrow::RecordBatchVector& record_batches) {
  std::unordered_map<std::string, arrow::RecordBatchVector> grouped;
  for (auto& record_batch : record_batches) {
    auto group_string = metadata::extractGroupMetadata(record_batch);

    if (grouped.find(group_string) == grouped.end()) {
      grouped[group_string] = arrow::RecordBatchVector();
    }

    grouped[group_string].push_back(record_batch);
  }

  return grouped;
}
arrow::Status AggregateHandler::isValid(
    const arrow::RecordBatchVector& record_batches) const {
  std::string time_column_name{""};

  for (auto& record_batch : record_batches) {
    if (time_column_name.empty()) {
      ARROW_RETURN_NOT_OK(metadata::getTimeColumnNameMetadata(
          record_batch, &time_column_name));
    }

    auto time_column = record_batch->GetColumnByName(time_column_name);

    if (time_column == nullptr) {
      return arrow::Status::Invalid(fmt::format(
          "Time column with name {} should be presented", time_column_name));
    }

    if (time_column->type_id() != arrow::Type::TIMESTAMP) {
      return arrow::Status::NotImplemented(
          "Aggregation currently supports arrow::Type::TIMESTAMP type for "
          "timestamp "
          "field only");  // TODO: support any numeric type
    }
  }

  return arrow::Status::OK();
}
arrow::Status AggregateHandler::fillMeasurementColumn(
    const arrow::RecordBatchVector& grouped,
    arrow::ArrayVector* result_arrays,
    const std::string& measurement_column_name) {
  arrow::StringBuilder measurement_column_builder;
  for (auto& record_batch : grouped) {
    auto measurement_column =
        record_batch->GetColumnByName(measurement_column_name);

    if (measurement_column == nullptr) {
      return arrow::Status::Invalid(fmt::format(
          "Measurement column {} is not present", measurement_column_name));
    }

    auto measurement_value_result = measurement_column->GetScalar(0);
    ARROW_RETURN_NOT_OK(measurement_value_result.status());

    ARROW_RETURN_NOT_OK(measurement_column_builder.Append(
        measurement_value_result.ValueOrDie()->ToString()));
  }

  result_arrays->emplace_back();

  ARROW_RETURN_NOT_OK(
      measurement_column_builder.Finish(&result_arrays->back()));

  return arrow::Status::OK();
}

}  // namespace stream_data_processor
