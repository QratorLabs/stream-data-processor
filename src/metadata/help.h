#pragma once

#include <memory>
#include <string>

#include <arrow/api.h>

namespace stream_data_processor {
namespace metadata {
namespace help {

arrow::Status setFieldMetadata(std::shared_ptr<arrow::Field>* field,
                               const std::string& key,
                               const std::string& metadata);

arrow::Status replaceField(std::shared_ptr<arrow::RecordBatch>* record_batch,
                           const std::shared_ptr<arrow::Field>& field);

arrow::Status setColumnMetadata(
    std::shared_ptr<arrow::RecordBatch>* record_batch, int i,
    const std::string& key, const std::string& metadata);

arrow::Status setColumnMetadata(
    std::shared_ptr<arrow::RecordBatch>* record_batch,
    const std::string& column_name, const std::string& key,
    const std::string& metadata);

arrow::Status setSchemaMetadata(
    std::shared_ptr<arrow::RecordBatch>* record_batch, const std::string& key,
    const std::string& metadata);

arrow::Result<std::string> getFieldMetadata(const arrow::Field& field,
                                            const std::string& key);

arrow::Result<std::string> getColumnNameMetadata(
    const arrow::RecordBatch& record_batch, const std::string& metadata_key);

}  // namespace help
}  // namespace metadata
}  // namespace stream_data_processor
