#include <map>

#include <spdlog/spdlog.h>

#include "grouping.h"

const std::string RecordBatchGrouping::METADATA_KEY{"group"};

arrow::Status RecordBatchGrouping::fillGroupMetadata(
    std::shared_ptr<arrow::RecordBatch>* record_batch,
    const std::vector<std::string>& grouping_columns) {
  std::map<std::string, std::string> group_columns_values;
  for (auto& grouping_column : grouping_columns) {
    auto column_value_result =
        record_batch->get()->GetColumnByName(grouping_column)->GetScalar(0);
    if (!column_value_result.ok()) {
      return column_value_result.status();
    }

    group_columns_values[grouping_column] =
        column_value_result.ValueOrDie()->ToString();
  }

  RecordBatchGroup group;
  for (auto& [column_name, column_string_value] : group_columns_values) {
    group.mutable_group_columns_names()->add_columns_names(column_name);
    auto new_value = group.mutable_group_columns_values()->Add();
    *new_value = column_string_value;
  }

  auto arrow_metadata = std::make_shared<arrow::KeyValueMetadata>();
  ARROW_RETURN_NOT_OK(arrow_metadata->Set(
      METADATA_KEY, group.SerializeAsString()
  ));
  *record_batch = record_batch->get()->ReplaceSchemaMetadata(arrow_metadata);

  return arrow::Status::OK();
}

std::string RecordBatchGrouping::extractGroupMetadata(
    const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  if (!record_batch->schema()->metadata()->Contains(METADATA_KEY)) {
    return "";
  }

  return record_batch->schema()->metadata()->Get(METADATA_KEY).ValueOrDie();
}

RecordBatchGroup RecordBatchGrouping::extractGroup(
    const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  RecordBatchGroup group;
  if (!record_batch->schema()->HasMetadata()) {
    return group;
  }

  auto metadata = record_batch->schema()->metadata();
  if (!metadata->Contains(METADATA_KEY)) {
    return group;
  }

  group.ParseFromString(metadata->Get(METADATA_KEY).ValueOrDie());
  return group;
}

std::vector<std::string> RecordBatchGrouping::extractGroupingColumnsNames(
    const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  std::vector<std::string> grouping_columns_names;
  auto group = extractGroup(record_batch);
  for (auto& group_column : group.group_columns_names().columns_names()) {
    grouping_columns_names.push_back(group_column);
  }

  return grouping_columns_names;
}

std::string RecordBatchGrouping::getGroupingColumnsSetKey(
    const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  auto group = extractGroup(record_batch);
  return group.group_columns_names().SerializeAsString();
}
