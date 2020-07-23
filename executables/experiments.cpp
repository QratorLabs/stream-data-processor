#include <iostream>

#include <arrow/api.h>

#include "utils.h"

int main() {
  std::shared_ptr<arrow::RecordBatch> record_batch;
  arrow::Int64Builder builder;
  auto append_status = builder.AppendValues({105, 110, 115, 120, 125, 130, 135, 140, 145, 150});
  if (!append_status.ok()) {
    std::cerr << append_status.ToString() << std::endl;
    return 1;
  }

  std::shared_ptr<arrow::Array> array;
  auto build_status = builder.Finish(&array);
  if (!build_status.ok()) {
    std::cerr << build_status.ToString() << std::endl;
    return 1;
  }

  auto schema = arrow::schema({arrow::field("ts", arrow::int64())});

  record_batch = arrow::RecordBatch::Make(schema, array->length(), {array});

  std::cout << "Before serialization:\n";
  std::cout << record_batch->ToString() << std::endl;

  std::shared_ptr<arrow::Buffer> buffer;
  auto serialization_status = Utils::serializeRecordBatches(schema, {record_batch}, &buffer);
  if (!serialization_status.ok()) {
    std::cerr << serialization_status.ToString() << std::endl;
    return 1;
  }

  std::cout << "Buffer size: " << buffer->size() << std::endl;

  arrow::RecordBatchVector record_batch_vector;
  auto deserialization_status = Utils::deserializeRecordBatches(buffer, &record_batch_vector);
  if (!deserialization_status.ok()) {
    std::cerr << deserialization_status.ToString() << std::endl;
    return 1;
  }

  std::cout << "After deserialization:\n";
  for (auto& record : record_batch_vector) {
    std::cout << record->ToString() << std::endl;
  }

  return 0;
}