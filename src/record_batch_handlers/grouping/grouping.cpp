#include <map>

#include <spdlog/spdlog.h>

#include "grouping.h"

const std::string RecordBatchGrouping::METADATA_KEY{"group"};

arrow::Status RecordBatchGrouping::fillGroupMetadata(
    std::shared_ptr<arrow::RecordBatch>* record_batch,
    const std::vector<std::string>& grouping_columns) {
  std::map<std::string, std::string> group_columns_values;

  ARROW_RETURN_NOT_OK(fillGroupMap(
      &group_columns_values,
      *record_batch,
      extractGroupingColumnsNames(*record_batch)
      ));

  ARROW_RETURN_NOT_OK(fillGroupMap(
      &group_columns_values,
      *record_batch,
      grouping_columns
      ));

  RecordBatchGroup group;
  for (auto& [column_name, column_string_value] : group_columns_values) {
    group.mutable_group_columns_names()->add_columns_names(column_name);
    auto new_value = group.mutable_group_columns_values()->Add();
    *new_value = column_string_value;
  }

  std::shared_ptr<arrow::KeyValueMetadata> arrow_metadata = nullptr;
  if (record_batch->get()->schema()->HasMetadata()) {
    arrow_metadata = record_batch->get()->schema()->metadata()->Copy();
  } else {
    arrow_metadata = std::make_shared<arrow::KeyValueMetadata>();
  }

  ARROW_RETURN_NOT_OK(arrow_metadata->Set(
      METADATA_KEY, group.SerializeAsString()
  ));
  *record_batch = record_batch->get()->ReplaceSchemaMetadata(arrow_metadata);

  return arrow::Status::OK();
}

std::string RecordBatchGrouping::extractGroupMetadata(
    const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  if (!record_batch->schema()->HasMetadata()) {
    return "";
  }

  auto metadata = record_batch->schema()->metadata();
  if (!metadata->Contains(METADATA_KEY)) {
    return "";
  }

  return metadata->Get(METADATA_KEY).ValueOrDie();
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

arrow::Status RecordBatchGrouping::fillGroupMap(
    std::map<std::string, std::string>* group_map,
    const std::shared_ptr<arrow::RecordBatch>& record_batch,
    const std::vector<std::string>& grouping_columns) {
  for (auto& grouping_column_name : grouping_columns) {
    if (group_map->find(grouping_column_name) != group_map->end()) {
      continue;
    }

    auto column = record_batch->GetColumnByName(grouping_column_name);
    if (column == nullptr) {
      continue;
    }

    auto column_value_result = column->GetScalar(0);
    if (!column_value_result.ok()) {
      return column_value_result.status();
    }

    group_map->operator[](grouping_column_name) =
        column_value_result.ValueOrDie()->ToString();
  }

  return arrow::Status::OK();
}
