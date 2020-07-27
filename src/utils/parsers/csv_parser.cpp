#include <arrow/csv/api.h>
#include <arrow/io/api.h>

#include "csv_parser.h"

CSVParser::CSVParser(std::shared_ptr<arrow::Schema> schema) : record_batches_schema_(std::move(schema)) {

}

arrow::Status CSVParser::parseRecordBatches(const std::shared_ptr<arrow::Buffer>& buffer,
                                            std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batches) {
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  auto buffer_input = std::make_shared<arrow::io::BufferReader>(buffer);

  auto read_options = arrow::csv::ReadOptions::Defaults();
  auto parse_options = arrow::csv::ParseOptions::Defaults();
  auto convert_options = arrow::csv::ConvertOptions::Defaults();

  bool read_column_names = record_batches_schema_ == nullptr;
  read_options.autogenerate_column_names = !read_column_names;

  auto batch_reader_result = arrow::csv::StreamingReader::Make(pool, buffer_input, read_options,
                                                               parse_options, convert_options);
  if (!batch_reader_result.ok()) {
    return batch_reader_result.status();
  }

  record_batches.clear();
  ARROW_RETURN_NOT_OK(batch_reader_result.ValueOrDie()->ReadAll(&record_batches));
  if (read_column_names && !record_batches.empty()) {
    record_batches_schema_ = record_batches.front()->schema();
  } else {
    for (auto& record_batch : record_batches) {
      record_batch = arrow::RecordBatch::Make(record_batches_schema_,
                                              record_batch->num_rows(),
                                              record_batch->columns());
    }
  }

  ARROW_RETURN_NOT_OK(buffer_input->Close());
  return arrow::Status::OK();
}
