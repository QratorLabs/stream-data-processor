#include "serialize_utils.h"

namespace stream_data_processor {
namespace serialize_utils {

arrow::Result<arrow::BufferVector> serializeRecordBatches(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batches) {
  arrow::BufferVector result;

  for (auto& record_batch : record_batches) {
    ARROW_ASSIGN_OR_RAISE(auto output_stream,
                          arrow::io::BufferOutputStream::Create());

    std::shared_ptr<arrow::ipc::RecordBatchWriter> stream_writer;
    ARROW_ASSIGN_OR_RAISE(
        stream_writer, arrow::ipc::MakeStreamWriter(output_stream.get(),
                                                    record_batch->schema()));

    ARROW_RETURN_NOT_OK(stream_writer->WriteRecordBatch(*record_batch));
    ARROW_ASSIGN_OR_RAISE(auto buffer, output_stream->Finish());
    result.push_back(buffer);
    ARROW_RETURN_NOT_OK(output_stream->Close());
  }

  return result;
}

arrow::Result<arrow::RecordBatchVector> deserializeRecordBatches(
    const arrow::Buffer& buffer) {
  auto buffer_input = std::make_shared<arrow::io::BufferReader>(buffer);

  std::shared_ptr<arrow::ipc::RecordBatchStreamReader> batch_reader;
  ARROW_ASSIGN_OR_RAISE(
      batch_reader, arrow::ipc::RecordBatchStreamReader::Open(buffer_input));

  arrow::RecordBatchVector record_batches;
  ARROW_RETURN_NOT_OK(batch_reader->ReadAll(&record_batches));
  ARROW_RETURN_NOT_OK(buffer_input->Close());
  return record_batches;
}

}  // namespace serialize_utils
}  // namespace stream_data_processor
