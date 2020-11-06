#include "serialize_utils.h"

namespace stream_data_processor {
namespace serialize_utils {

arrow::Status serializeRecordBatches(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batches,
    std::vector<std::shared_ptr<arrow::Buffer>>* target) {
  for (auto& record_batch : record_batches) {
    auto output_stream_result = arrow::io::BufferOutputStream::Create();
    ARROW_RETURN_NOT_OK(output_stream_result.status());

    auto stream_writer_result = arrow::ipc::MakeStreamWriter(
        output_stream_result.ValueOrDie().get(), record_batch->schema());

    ARROW_RETURN_NOT_OK(stream_writer_result.status());

    ARROW_RETURN_NOT_OK(
        stream_writer_result.ValueOrDie()->WriteRecordBatch(*record_batch));

    auto buffer_result = output_stream_result.ValueOrDie()->Finish();
    ARROW_RETURN_NOT_OK(buffer_result.status());

    target->push_back(buffer_result.ValueOrDie());
    ARROW_RETURN_NOT_OK(output_stream_result.ValueOrDie()->Close());
  }

  return arrow::Status::OK();
}

arrow::Status deserializeRecordBatches(
    const std::shared_ptr<arrow::Buffer>& buffer,
    std::vector<std::shared_ptr<arrow::RecordBatch>>* record_batches) {
  auto buffer_input = std::make_shared<arrow::io::BufferReader>(buffer);

  auto batch_reader_result =
      arrow::ipc::RecordBatchStreamReader::Open(buffer_input);

  ARROW_RETURN_NOT_OK(batch_reader_result.status());

  ARROW_RETURN_NOT_OK(
      batch_reader_result.ValueOrDie()->ReadAll(record_batches));

  ARROW_RETURN_NOT_OK(buffer_input->Close());
  return arrow::Status::OK();
}

}  // namespace serialize_utils
}  // namespace stream_data_processor
