#pragma once

#include <iostream>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

struct IPv4Endpoint {
 std::string host;
 uint16_t port;
};

class Utils {
 public:
  static arrow::Status CSVToTable(std::shared_ptr<arrow::Buffer> buffer, std::shared_ptr<arrow::Table>* table) {
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    auto buffer_input = std::make_shared<arrow::io::BufferReader>(buffer);

    auto read_options = arrow::csv::ReadOptions::Defaults();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    auto convert_options = arrow::csv::ConvertOptions::Defaults();

    auto table_reader_result = arrow::csv::TableReader::Make(pool, buffer_input, read_options,
                                                             parse_options, convert_options);
    ARROW_RETURN_NOT_OK(table_reader_result);
    auto table_result = table_reader_result.ValueOrDie()->Read();
    ARROW_RETURN_NOT_OK(table_result);
    *table = table_result.ValueOrDie();

    ARROW_RETURN_NOT_OK(buffer_input->Close());
    return arrow::Status::OK();
  }

  static arrow::Status CSVToRecordBatches(std::shared_ptr<arrow::Buffer> buffer,
      std::vector<std::shared_ptr<arrow::RecordBatch>>* record_batches) {
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    auto buffer_input = std::make_shared<arrow::io::BufferReader>(buffer);

    auto read_options = arrow::csv::ReadOptions::Defaults();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    auto convert_options = arrow::csv::ConvertOptions::Defaults();

    auto batch_reader_result = arrow::csv::StreamingReader::Make(pool, buffer_input, read_options,
        parse_options, convert_options);

    ARROW_RETURN_NOT_OK(batch_reader_result);
    ARROW_RETURN_NOT_OK(batch_reader_result.ValueOrDie()->ReadAll(record_batches));

    ARROW_RETURN_NOT_OK(buffer_input->Close());
    return arrow::Status::OK();
  }

  static arrow::Status BufferToRecordBatches(std::shared_ptr<arrow::Buffer> buffer,
                                             std::vector<std::shared_ptr<arrow::RecordBatch>>* record_batches) {
    auto buffer_input = std::make_shared<arrow::io::BufferReader>(buffer);
    auto read_options = arrow::ipc::IpcReadOptions::Defaults();
    auto batch_reader_result = arrow::ipc::RecordBatchStreamReader::Open(buffer_input, read_options);

    ARROW_RETURN_NOT_OK(batch_reader_result);
    ARROW_RETURN_NOT_OK(batch_reader_result.ValueOrDie()->ReadAll(record_batches));

    ARROW_RETURN_NOT_OK(buffer_input->Close());
    return arrow::Status::OK();
  }
};
