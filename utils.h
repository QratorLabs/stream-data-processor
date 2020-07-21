#pragma once

#include <iostream>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

struct IPv4Endpoint {
 std::string host;
 uint16_t port;
};

class Utils {
 public:
  static arrow::Status parseCSVToTable(std::shared_ptr<arrow::Buffer> buffer, std::shared_ptr<arrow::Table>* table) {
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

  static arrow::Status convertTableToRecordBatch(const std::shared_ptr<arrow::Table>& table,
      std::shared_ptr<arrow::RecordBatch>* record_batch) {
    auto prepared_table_result = table->CombineChunks();
    if (!prepared_table_result.ok()) {
      return prepared_table_result.status();
    }

    arrow::ArrayVector table_columns;
    if (table->num_rows() != 0) {
      for (auto &column : prepared_table_result.ValueOrDie()->columns()) {
        table_columns.push_back(column->chunk(0));
      }
    }

    *record_batch = arrow::RecordBatch::Make(prepared_table_result.ValueOrDie()->schema(),
        prepared_table_result.ValueOrDie()->num_rows(),
        table_columns);
    return arrow::Status::OK();
  }

  static arrow::Status concatenateRecordBatches(const std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batches,
      std::shared_ptr<arrow::RecordBatch>* target) {
    auto table_result = arrow::Table::FromRecordBatches(record_batches);
    if (!table_result.ok()) {
      return table_result.status();
    }

    ARROW_RETURN_NOT_OK(convertTableToRecordBatch(table_result.ValueOrDie(), target));
    return arrow::Status::OK();
  }

  static arrow::Status groupSortingByColumns(const std::vector<std::string>& column_names,
      const std::shared_ptr<arrow::RecordBatch>& record_batch,
      std::vector<std::shared_ptr<arrow::RecordBatch>>& grouped) {
    return sort(column_names, 0, record_batch, grouped);
  }

  static arrow::Status sortByColumn(const std::string& column_name,
                                    const std::shared_ptr<arrow::RecordBatch>& source,
                                    std::shared_ptr<arrow::RecordBatch>* target) {
    auto sorting_column = source->GetColumnByName(column_name);
    if (sorting_column->type_id() == arrow::Type::TIMESTAMP) {
      auto converted_sorting_column_result = sorting_column->View(arrow::int64());
      if (!converted_sorting_column_result.ok()) {
        return converted_sorting_column_result.status();
      }

      sorting_column = converted_sorting_column_result.ValueOrDie();
    }

    auto sorted_idx_result = arrow::compute::SortToIndices(*sorting_column);
    if (!sorted_idx_result.ok()) {
      return sorted_idx_result.status();
    }

    auto sorted_record_batch_result = arrow::compute::Take(source, sorted_idx_result.ValueOrDie());
    if (!sorted_record_batch_result.ok()) {
      return sorted_record_batch_result.status();
    }

    *target = sorted_record_batch_result.ValueOrDie().record_batch();
    return arrow::Status::OK();
  }

  static arrow::Status argMinMax(std::shared_ptr<arrow::Array> array, std::pair<size_t, size_t>& arg_min_max) {
    if (array->type_id() == arrow::Type::TIMESTAMP) {
      auto converted_sorting_column_result = array->View(arrow::int64());
      if (!converted_sorting_column_result.ok()) {
        return converted_sorting_column_result.status();
      }

      array = converted_sorting_column_result.ValueOrDie();
    }

    auto min_max_ts_result = arrow::compute::MinMax(array);
    if (!min_max_ts_result.ok()) {
      return min_max_ts_result.status();
    }

    int64_t arg_min = -1;
    int64_t arg_max = -1;
    size_t i = 0;
    while (i < array->length() && (arg_min == -1 || arg_max == -1)) {
      auto get_scalar_result = array->GetScalar(i);
      if (!get_scalar_result.ok()) {
        return get_scalar_result.status();
      }

      if (get_scalar_result.ValueOrDie()->Equals(min_max_ts_result.ValueOrDie().scalar_as<arrow::StructScalar>().value[0])) {
        arg_min = i;
      }

      if (get_scalar_result.ValueOrDie()->Equals(min_max_ts_result.ValueOrDie().scalar_as<arrow::StructScalar>().value[1])) {
        arg_max = i;
      }

      ++i;
    }

    arg_min_max = {arg_min, arg_max};
    return arrow::Status::OK();
  }

 private:
  static arrow::Status sort(const std::vector<std::string>& column_names,
      size_t i,
      const std::shared_ptr<arrow::RecordBatch>& source,
      std::vector<std::shared_ptr<arrow::RecordBatch>> &targets) {
    if (i == column_names.size()) {
      targets.push_back(source);
      return arrow::Status::OK();
    }

    std::shared_ptr<arrow::RecordBatch> sorted_batch;
    ARROW_RETURN_NOT_OK(sortByColumn(column_names[i], source, &sorted_batch));
    while (true) {
      auto sorted_keys = sorted_batch->GetColumnByName(column_names[i]);

      auto min_val = sorted_keys->GetScalar(0);
      if (!min_val.ok()) {
        return min_val.status();
      }

      auto max_val = sorted_keys->GetScalar(sorted_keys->length() - 1);
      if (!max_val.ok()) {
        return max_val.status();
      }

      auto equals_result = arrow::compute::Compare(sorted_keys, min_val.ValueOrDie(),
                                                   arrow::compute::CompareOptions(arrow::compute::CompareOperator::EQUAL));
      if (!equals_result.ok()) {
        return equals_result.status();
      }

      auto filter_equals_result = arrow::compute::Filter(sorted_batch, equals_result.ValueOrDie());
      if (!filter_equals_result.ok()) {
        return filter_equals_result.status();
      }

      ARROW_RETURN_NOT_OK(sort(column_names, i + 1, filter_equals_result.ValueOrDie().record_batch(), targets));
      if (min_val.ValueOrDie()->Equals(max_val.ValueOrDie())) {
        break;
      }

      auto not_equals_result = arrow::compute::Compare(sorted_keys, min_val.ValueOrDie(),
                                                       arrow::compute::CompareOptions(arrow::compute::CompareOperator::NOT_EQUAL));
      if (!not_equals_result.ok()) {
        return equals_result.status();
      }

      auto filter_not_equals_result = arrow::compute::Filter(sorted_batch, not_equals_result.ValueOrDie());
      if (!filter_not_equals_result.ok()) {
        return filter_not_equals_result.status();
      }

      sorted_batch = filter_not_equals_result.ValueOrDie().record_batch();
    }

    return arrow::Status::OK();
  }
};
