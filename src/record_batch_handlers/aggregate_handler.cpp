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
                         std::unique_ptr<AggregateFunction>>
    AggregateHandler::TYPES_TO_FUNCTIONS = []() {
      std::unordered_map<AggregateHandler::AggregateFunctionEnumType,
                         std::unique_ptr<AggregateFunction>>
          init_map;

      init_map[kFirst] = std::make_unique<FirstAggregateFunction>();
      init_map[kLast] = std::make_unique<LastAggregateFunction>();
      init_map[kMax] = std::make_unique<MaxAggregateFunction>();
      init_map[kMin] = std::make_unique<MinAggregateFunction>();
      init_map[kMean] = std::make_unique<MeanAggregateFunction>();
      return init_map;
    }();

AggregateHandler::AggregateHandler(
    const AggregateHandler::AggregateOptions& options)
    : options_(options) {}

AggregateHandler::AggregateHandler(
    AggregateHandler::AggregateOptions&& options)
    : options_(std::move(options)) {}

arrow::Result<arrow::RecordBatchVector> AggregateHandler::handle(
    const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  if (record_batch->num_rows() == 0) {
    return arrow::RecordBatchVector{};
  }

  ARROW_ASSIGN_OR_RAISE(auto result_vector,
                        handle(std::vector{record_batch}));
  if (result_vector.size() != 1) {
    return arrow::Status::ExecutionError(
        "Aggregation of one record batch "
        "should contain only one record");
  }

  return result_vector;
}

arrow::Result<arrow::RecordBatchVector> AggregateHandler::handle(
    const arrow::RecordBatchVector& record_batches) {
  if (record_batches.empty()) {
    return arrow::RecordBatchVector{};
  }

  ARROW_RETURN_NOT_OK(isValid(record_batches));

  auto grouped = splitByGroups(record_batches);

  arrow::RecordBatchVector result;
  for ([[maybe_unused]] auto& [_, record_batches_group] : grouped) {
    if (record_batches_group.empty()) {
      return arrow::Status::ExecutionError(
          "RecordBatchVector after splitting"
          " by groups must be non-empty");
    }

    auto pool = arrow::default_memory_pool();
    arrow::ArrayVector result_arrays;

    auto grouping_columns =
        metadata::extractGroupingColumnsNames(*record_batches_group.front());

    std::string time_column_name;
    ARROW_ASSIGN_OR_RAISE(
        time_column_name,
        metadata::getTimeColumnNameMetadata(*record_batches.front()));

    for (auto group_iter = grouping_columns.begin();
         group_iter != grouping_columns.end(); ++group_iter) {
      if (*group_iter == time_column_name) {
        grouping_columns.erase(
            group_iter);  // we should remove time grouping -- time column
                          // will be replaced with aggregated one
        break;
      }
    }

    bool explicitly_add_measurement_field = true;
    bool has_measurement = false;

    std::string measurement_column_name;
    auto measurement_column_name_result =
        metadata::getMeasurementColumnNameMetadata(*record_batches.front());

    if (!measurement_column_name_result.ok()) {
      explicitly_add_measurement_field = false;
    } else {
      has_measurement = true;
      measurement_column_name =
          std::move(measurement_column_name_result).ValueOrDie();
      for (auto& grouping_column : grouping_columns) {
        if (grouping_column == measurement_column_name) {
          explicitly_add_measurement_field = false;
          break;
        }
      }
    }

    std::shared_ptr<arrow::Schema> result_schema;
    ARROW_ASSIGN_OR_RAISE(
        result_schema,
        createResultSchema(record_batches, grouping_columns,
                           explicitly_add_measurement_field,
                           measurement_column_name, time_column_name));

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

    result.push_back(arrow::RecordBatch::Make(
        result_schema, record_batches_group.size(), result_arrays));

    ARROW_RETURN_NOT_OK(
        metadata::fillGroupMetadata(&result.back(), grouping_columns));

    ARROW_RETURN_NOT_OK(metadata::setTimeColumnNameMetadata(
        &result.back(), options_.result_time_column_rule.result_column_name));

    if (has_measurement) {
      ARROW_RETURN_NOT_OK(metadata::setMeasurementColumnNameMetadata(
          &result.back(), measurement_column_name));
    }
  }

  return result;
}

arrow::Result<std::shared_ptr<arrow::Schema>>
AggregateHandler::createResultSchema(
    const arrow::RecordBatchVector& record_batches,
    const std::vector<std::string>& grouping_columns,
    bool explicitly_add_measurement,
    const std::string& measurement_column_name,
    const std::string& time_column_name) const {
  arrow::FieldVector result_fields;

  auto time_column_type =
      record_batches.front()->GetColumnByName(time_column_name)->type();

  result_fields.push_back(arrow::field(
      options_.result_time_column_rule.result_column_name, time_column_type));

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

  return arrow::schema(result_fields);
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
  if (aggregate_column_field->type()->id() == arrow::Type::TIMESTAMP) {
    ARROW_ASSIGN_OR_RAISE(builder, arrow_utils::createTimestampArrayBuilder(
                                       aggregate_column_field->type(), pool));
  } else {
    ARROW_ASSIGN_OR_RAISE(builder,
                          arrow_utils::createArrayBuilder(
                              aggregate_column_field->type()->id(), pool));
  }

  for (auto& group : groups) {
    ARROW_ASSIGN_OR_RAISE(auto aggregated_value,
                          TYPES_TO_FUNCTIONS.at(aggregate_function)
                              ->aggregate(*group, aggregate_column_name));

    if (aggregate_column_field->type()->id() == arrow::Type::TIMESTAMP) {
      ARROW_RETURN_NOT_OK(arrow_utils::appendToTimestampBuilder(
          aggregated_value, &builder, aggregate_column_field->type()));
    } else {
      ARROW_RETURN_NOT_OK(arrow_utils::appendToBuilder(
          aggregated_value, &builder, aggregate_column_field->type()->id()));
    }
  }

  result_arrays->emplace_back();
  ARROW_RETURN_NOT_OK(builder->Finish(&result_arrays->back()));
  return arrow::Status::OK();
}

arrow::Status AggregateHandler::aggregateTimeColumn(
    const arrow::RecordBatchVector& record_batch_vector,
    arrow::ArrayVector* result_arrays, arrow::MemoryPool* pool) const {
  ARROW_ASSIGN_OR_RAISE(
      auto time_column_name,
      metadata::getTimeColumnNameMetadata(*record_batch_vector.front()));

  auto ts_column_type =
      record_batch_vector.front()->GetColumnByName(time_column_name)->type();

  arrow::NumericBuilder<arrow::TimestampType> ts_builder(ts_column_type,
                                                         pool);
  for (auto& group : record_batch_vector) {
    std::shared_ptr<arrow::Scalar> ts;

    ARROW_ASSIGN_OR_RAISE(
        ts, TYPES_TO_FUNCTIONS
                .at(options_.result_time_column_rule.aggregate_function)
                ->aggregate(*group, time_column_name));

    ARROW_ASSIGN_OR_RAISE(ts, ts->CastTo(ts_column_type));
    ARROW_RETURN_NOT_OK(ts_builder.Append(
        std::static_pointer_cast<arrow::TimestampScalar>(ts)->value));
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
      ARROW_ASSIGN_OR_RAISE(cropped_record_batch,
                            cropped_record_batch->RemoveColumn(i));

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
  ARROW_ASSIGN_OR_RAISE(
      unique_record_batch,
      convert_utils::concatenateRecordBatches(cropped_groups));

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
    auto group_string = metadata::extractGroupMetadata(*record_batch);

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
      ARROW_ASSIGN_OR_RAISE(
          time_column_name,
          metadata::getTimeColumnNameMetadata(*record_batch));
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

    ARROW_ASSIGN_OR_RAISE(auto measurement_value,
                          measurement_column->GetScalar(0));

    ARROW_RETURN_NOT_OK(
        measurement_column_builder.Append(measurement_value->ToString()));
  }

  result_arrays->emplace_back();

  ARROW_RETURN_NOT_OK(
      measurement_column_builder.Finish(&result_arrays->back()));

  return arrow::Status::OK();
}

}  // namespace stream_data_processor
