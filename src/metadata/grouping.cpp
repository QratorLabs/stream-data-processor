#include <map>

#include <spdlog/spdlog.h>

#include "grouping.h"
#include "help.h"

namespace stream_data_processor {
namespace metadata {

namespace {

inline const std::string GROUP_METADATA_KEY{"group"};

arrow::Status fillGroupMap(std::map<std::string, std::string>* group_map,
                           const arrow::RecordBatch& record_batch,
                           const std::vector<std::string>& grouping_columns) {
  for (auto& grouping_column_name : grouping_columns) {
    if (group_map->find(grouping_column_name) != group_map->end()) {
      continue;
    }

    auto column = record_batch.GetColumnByName(grouping_column_name);
    if (column == nullptr) {
      continue;
    }

    ARROW_ASSIGN_OR_RAISE(auto column_value, column->GetScalar(0));
    (*group_map)[grouping_column_name] = column_value->ToString();
  }

  return arrow::Status::OK();
}

}  // namespace

arrow::Status fillGroupMetadata(
    std::shared_ptr<arrow::RecordBatch>* record_batch,
    const std::vector<std::string>& grouping_columns) {
  std::map<std::string, std::string> group_columns_values;

  ARROW_RETURN_NOT_OK(
      fillGroupMap(&group_columns_values, **record_batch,
                   extractGroupingColumnsNames(**record_batch)));

  ARROW_RETURN_NOT_OK(
      fillGroupMap(&group_columns_values, **record_batch, grouping_columns));

  auto group = constructGroupFromOrderedMap(group_columns_values);
  ARROW_RETURN_NOT_OK(setGroupMetadata(record_batch, group));

  return arrow::Status::OK();
}

std::string extractGroupMetadata(const arrow::RecordBatch& record_batch) {
  if (!record_batch.schema()->HasMetadata()) {
    return "";
  }

  auto metadata = record_batch.schema()->metadata();
  if (!metadata->Contains(GROUP_METADATA_KEY)) {
    return "";
  }

  return metadata->Get(GROUP_METADATA_KEY).ValueOrDie();
}

RecordBatchGroup extractGroup(const arrow::RecordBatch& record_batch) {
  RecordBatchGroup group;
  if (!record_batch.schema()->HasMetadata()) {
    return group;
  }

  auto metadata = record_batch.schema()->metadata();
  if (!metadata->Contains(GROUP_METADATA_KEY)) {
    return group;
  }

  group.ParseFromString(metadata->Get(GROUP_METADATA_KEY).ValueOrDie());
  return group;
}

std::vector<std::string> extractGroupingColumnsNames(
    const arrow::RecordBatch& record_batch) {
  std::vector<std::string> grouping_columns_names;
  auto group = extractGroup(record_batch);
  for (auto& group_column : group.group_columns_names().columns_names()) {
    grouping_columns_names.push_back(group_column);
  }

  return grouping_columns_names;
}

std::string getGroupingColumnsSetKey(const arrow::RecordBatch& record_batch) {
  auto group = extractGroup(record_batch);
  return group.group_columns_names().SerializeAsString();
}

RecordBatchGroup constructGroupFromOrderedMap(
    const std::map<std::string, std::string>& group_map) {
  RecordBatchGroup group;
  for (auto& [column_name, column_string_value] : group_map) {
    group.mutable_group_columns_names()->add_columns_names(column_name);
    auto new_value = group.mutable_group_columns_values()->Add();
    *new_value = column_string_value;
  }

  return group;
}
arrow::Status setGroupMetadata(
    std::shared_ptr<arrow::RecordBatch>* record_batch,
    const RecordBatchGroup& group) {
  ARROW_RETURN_NOT_OK(help::setSchemaMetadata(
      record_batch, GROUP_METADATA_KEY, group.SerializeAsString()));

  return arrow::Status::OK();
}

}  // namespace metadata
}  // namespace stream_data_processor
