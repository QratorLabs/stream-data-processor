#include "serializer.h"

arrow::Status Serializer::serializeRecordBatches(
    const std::shared_ptr<arrow::Schema>& schema,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batches,
    std::shared_ptr<arrow::Buffer>* target) {
  auto output_stream_result = arrow::io::BufferOutputStream::Create();
  if (!output_stream_result.ok()) {
    return output_stream_result.status();
  }

  auto stream_writer_result = arrow::ipc::MakeStreamWriter(
      output_stream_result.ValueOrDie().get(), schema);
  if (!stream_writer_result.ok()) {
    return stream_writer_result.status();
  }

  for (auto& record_batch : record_batches) {
    ARROW_RETURN_NOT_OK(
        stream_writer_result.ValueOrDie()->WriteRecordBatch(*record_batch));
  }

  auto buffer_result = output_stream_result.ValueOrDie()->Finish();
  if (!buffer_result.ok()) {
    return buffer_result.status();
  }

  *target = buffer_result.ValueOrDie();
  ARROW_RETURN_NOT_OK(output_stream_result.ValueOrDie()->Close());
  return arrow::Status::OK();
}

arrow::Status Serializer::deserializeRecordBatches(
    const std::shared_ptr<arrow::Buffer>& buffer,
    std::vector<std::shared_ptr<arrow::RecordBatch>>* record_batches) {
  auto buffer_input = std::make_shared<arrow::io::BufferReader>(buffer);
  auto batch_reader_result =
      arrow::ipc::RecordBatchStreamReader::Open(buffer_input);
  if (!batch_reader_result.ok()) {
    return batch_reader_result.status();
  }

  record_batches->clear();
  ARROW_RETURN_NOT_OK(
      batch_reader_result.ValueOrDie()->ReadAll(record_batches));

  ARROW_RETURN_NOT_OK(buffer_input->Close());
  return arrow::Status::OK();
}
