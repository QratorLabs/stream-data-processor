#include <arrow/csv/api.h>
#include <arrow/io/api.h>

#include "csv_parser.h"
#include "metadata/column_typing.h"

namespace stream_data_processor {

CSVParser::CSVParser(std::shared_ptr<arrow::Schema> schema)
    : record_batches_schema_(std::move(schema)) {}

arrow::Result<arrow::RecordBatchVector> CSVParser::parseRecordBatches(
    const arrow::Buffer& buffer) {
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  auto buffer_input = std::make_shared<arrow::io::BufferReader>(buffer);

  auto read_options = arrow::csv::ReadOptions::Defaults();
  auto parse_options = arrow::csv::ParseOptions::Defaults();
  auto convert_options = arrow::csv::ConvertOptions::Defaults();

  bool read_column_names = record_batches_schema_ == nullptr;
  read_options.autogenerate_column_names = !read_column_names;

  ARROW_ASSIGN_OR_RAISE(
      auto batch_reader,
      arrow::csv::StreamingReader::Make(pool, buffer_input, read_options,
                                        parse_options, convert_options));

  arrow::RecordBatchVector record_batches;
  ARROW_RETURN_NOT_OK(batch_reader->ReadAll(&record_batches));

  if (read_column_names && !record_batches.empty()) {
    record_batches_schema_ = record_batches.front()->schema();
    ARROW_RETURN_NOT_OK(tryFindTimeColumn());
  }

  for (auto& record_batch : record_batches) {
    record_batch = arrow::RecordBatch::Make(record_batches_schema_,
                                            record_batch->num_rows(),
                                            record_batch->columns());
  }

  ARROW_RETURN_NOT_OK(buffer_input->Close());
  return record_batches;
}

arrow::Status CSVParser::tryFindTimeColumn() {
  if (record_batches_schema_ == nullptr) {
    return arrow::Status::OK();
  }

  std::shared_ptr<arrow::Field> time_field = nullptr;
  for (auto& field : record_batches_schema_->fields()) {
    if (field->type()->id() == arrow::Type::TIMESTAMP) {
      if (time_field != nullptr) {
        return arrow::Status::OK();
      }

      time_field = field;
    }
  }

  if (time_field == nullptr) {
    return arrow::Status::OK();
  }

  ARROW_RETURN_NOT_OK(
      metadata::setColumnTypeMetadata(&time_field, metadata::TIME));

  ARROW_RETURN_NOT_OK(record_batches_schema_->SetField(
      record_batches_schema_->GetFieldIndex(time_field->name()), time_field));

  return arrow::Status::OK();
}

}  // namespace stream_data_processor
