#include <memory>

#include <arrow/api.h>
#include <spdlog/spdlog.h>

#include "help.h"

namespace stream_data_processor {
namespace metadata {
namespace help {

arrow::Status setFieldMetadata(std::shared_ptr<arrow::Field>* field,
                               const std::string& key,
                               const std::string& metadata) {
  std::shared_ptr<arrow::KeyValueMetadata> arrow_metadata = nullptr;
  if (field->get()->HasMetadata()) {
    arrow_metadata = field->get()->metadata()->Copy();
  } else {
    arrow_metadata = std::make_shared<arrow::KeyValueMetadata>();
  }

  ARROW_RETURN_NOT_OK(arrow_metadata->Set(key, metadata));

  *field = field->get()->WithMetadata(arrow_metadata);
  return arrow::Status::OK();
}

arrow::Status replaceField(std::shared_ptr<arrow::RecordBatch>* record_batch,
                           const std::shared_ptr<arrow::Field>& field) {
  auto field_index =
      record_batch->get()->schema()->GetFieldIndex(field->name());

  if (field_index == -1) {
    return arrow::Status::KeyError(fmt::format(
        "No such field or the choice is ambiguous: {}", field->name()));
  }

  ARROW_ASSIGN_OR_RAISE(
      auto new_schema,
      record_batch->get()->schema()->SetField(field_index, field));

  *record_batch =
      arrow::RecordBatch::Make(new_schema, record_batch->get()->num_rows(),
                               record_batch->get()->columns());

  return arrow::Status::OK();
}

arrow::Status setColumnMetadata(
    std::shared_ptr<arrow::RecordBatch>* record_batch, int i,
    const std::string& key, const std::string& metadata) {
  if (i < 0 || i >= record_batch->get()->num_columns()) {
    return arrow::Status::IndexError(
        fmt::format("Column index {} is out of bounds", i));
  }

  auto field = record_batch->get()->schema()->field(i);
  ARROW_RETURN_NOT_OK(setFieldMetadata(&field, key, metadata));
  ARROW_RETURN_NOT_OK(replaceField(record_batch, field));
  return arrow::Status::OK();
}

arrow::Status setColumnMetadata(
    std::shared_ptr<arrow::RecordBatch>* record_batch,
    const std::string& column_name, const std::string& key,
    const std::string& metadata) {
  auto i = record_batch->get()->schema()->GetFieldIndex(column_name);
  if (i == -1) {
    return arrow::Status::KeyError(
        fmt::format("No such column: {}", column_name));
  }

  ARROW_RETURN_NOT_OK(setColumnMetadata(record_batch, i, key, metadata));
  return arrow::Status::OK();
}

arrow::Status setSchemaMetadata(
    std::shared_ptr<arrow::RecordBatch>* record_batch, const std::string& key,
    const std::string& metadata) {
  std::shared_ptr<arrow::KeyValueMetadata> arrow_metadata = nullptr;
  if (record_batch->get()->schema()->HasMetadata()) {
    arrow_metadata = record_batch->get()->schema()->metadata()->Copy();
  } else {
    arrow_metadata = std::make_shared<arrow::KeyValueMetadata>();
  }

  ARROW_RETURN_NOT_OK(arrow_metadata->Set(key, metadata));
  *record_batch = record_batch->get()->ReplaceSchemaMetadata(arrow_metadata);
  return arrow::Status::OK();
}

arrow::Result<std::string> getFieldMetadata(const arrow::Field& field,
                                            const std::string& key) {
  auto metadata = field.metadata();
  if (metadata == nullptr) {
    return arrow::Status::Invalid("Field has no metadata");
  }

  if (!metadata->Contains(key)) {
    return arrow::Status::KeyError(
        fmt::format("Field's metadata has no key {}", key));
  }

  return metadata->Get(key);
}

arrow::Result<std::string> getColumnNameMetadata(
    const arrow::RecordBatch& record_batch, const std::string& metadata_key) {
  auto metadata = record_batch.schema()->metadata();
  if (metadata == nullptr) {
    return arrow::Status::Invalid("RecordBatch has no metadata");
  }

  if (!metadata->Contains(metadata_key)) {
    return arrow::Status::KeyError(
        fmt::format("RecordBatch's metadata has no key {}", metadata_key));
  }

  return metadata->Get(metadata_key);
}

}  // namespace help
}  // namespace metadata
}  // namespace stream_data_processor
