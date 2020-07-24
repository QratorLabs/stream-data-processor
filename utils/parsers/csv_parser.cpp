#include <arrow/csv/api.h>
#include <arrow/io/api.h>

#include "csv_parser.h"

arrow::Status CSVParser::parseRecordBatches(std::shared_ptr<arrow::Buffer> buffer,
                                            std::vector<std::shared_ptr<arrow::RecordBatch>> *record_batches,
                                            bool read_column_names) {
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  auto buffer_input = std::make_shared<arrow::io::BufferReader>(buffer);

  auto read_options = arrow::csv::ReadOptions::Defaults();
  read_options.autogenerate_column_names = !read_column_names;
  auto parse_options = arrow::csv::ParseOptions::Defaults();
  auto convert_options = arrow::csv::ConvertOptions::Defaults();

  auto batch_reader_result = arrow::csv::StreamingReader::Make(pool, buffer_input, read_options,
                                                               parse_options, convert_options);
  if (!batch_reader_result.ok()) {
    return batch_reader_result.status();
  }

  ARROW_RETURN_NOT_OK(batch_reader_result.ValueOrDie()->ReadAll(record_batches));

  ARROW_RETURN_NOT_OK(buffer_input->Close());
  return arrow::Status::OK();
}
