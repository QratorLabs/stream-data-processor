#include "grouping.h"

const std::string RecordBatchGrouping::METADATA_KEY{"group"};

arrow::Status RecordBatchGrouping::fillGroupMetadata(
    std::shared_ptr<arrow::RecordBatch>* record_batch,
    const std::vector<std::string>& grouping_columns) {
  RecordBatchGroup group_metadata;
  for (auto& grouping_column : grouping_columns) {
    auto column_value_result =
        record_batch->get()->GetColumnByName(grouping_column)->GetScalar(0);
    if (!column_value_result.ok()) {
      return column_value_result.status();
    }

    group_metadata.mutable_grouping_columns_values()->operator[](
        grouping_column) = column_value_result.ValueOrDie()->ToString();
  }

  auto arrow_metadata = std::make_shared<arrow::KeyValueMetadata>();
  ARROW_RETURN_NOT_OK(arrow_metadata->Set(
      METADATA_KEY, group_metadata.SerializeAsString()
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
  auto metadata = record_batch->schema()->metadata();
  if (!metadata->Contains(METADATA_KEY)) {
    return group;
  }

  group.ParseFromString(
      metadata->Get(METADATA_KEY).ValueOrDie());
  return group;
}
