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

  static arrow::Status parseCSVToRecordBatches(std::shared_ptr<arrow::Buffer> buffer,
                                               std::vector<std::shared_ptr<arrow::RecordBatch>>* record_batches,
                                               bool read_column_names = false) {
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    auto buffer_input = std::make_shared<arrow::io::BufferReader>(buffer);

    auto read_options = arrow::csv::ReadOptions::Defaults();
    read_options.autogenerate_column_names = !read_column_names;
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    auto convert_options = arrow::csv::ConvertOptions::Defaults();

    auto batch_reader_result = arrow::csv::StreamingReader::Make(pool, buffer_input, read_options,
        parse_options, convert_options);

    ARROW_RETURN_NOT_OK(batch_reader_result);
    ARROW_RETURN_NOT_OK(batch_reader_result.ValueOrDie()->ReadAll(record_batches));

    ARROW_RETURN_NOT_OK(buffer_input->Close());
    return arrow::Status::OK();
  }

  static arrow::Status serializeRecordBatches(const arrow::Schema& schema,
      const std::vector<std::shared_ptr<arrow::RecordBatch>> &record_batches,
      std::shared_ptr<arrow::Buffer> *target) {
    auto buffer_builder = std::make_shared<arrow::BufferBuilder>();

    std::shared_ptr<arrow::Buffer> schema_buffer;
    ARROW_RETURN_NOT_OK(serializeSchema(schema, &schema_buffer));
    ARROW_RETURN_NOT_OK(buffer_builder->Append(schema_buffer->data(), schema_buffer->size()));

    for (auto& record_batch : record_batches) {
      auto serialization_result = arrow::ipc::SerializeRecordBatch(*record_batch, arrow::ipc::IpcWriteOptions::Defaults());
      if (!serialization_result.ok()) {
        return serialization_result.status();
      }

      ARROW_RETURN_NOT_OK(buffer_builder->Append(serialization_result.ValueOrDie()->data(),
                                                 serialization_result.ValueOrDie()->size()));
    }

    ARROW_RETURN_NOT_OK(buffer_builder->Finish(target));
    return arrow::Status::OK();
  }

  static arrow::Status serializeSchema(const arrow::Schema& schema, std::shared_ptr<arrow::Buffer> *target) {
    arrow::ipc::DictionaryMemo dictionary_memo;
    auto schema_serialization_result = arrow::ipc::SerializeSchema(schema, &dictionary_memo);
    if (!schema_serialization_result.ok()) {
      return schema_serialization_result.status();
    }

    *target = schema_serialization_result.ValueOrDie();
    return arrow::Status::OK();
  }

  static arrow::Status deserializeRecordBatches(std::shared_ptr<arrow::Buffer> buffer,
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
