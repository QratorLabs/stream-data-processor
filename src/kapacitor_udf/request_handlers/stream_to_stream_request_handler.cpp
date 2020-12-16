#include "stream_to_stream_request_handler.h"

namespace stream_data_processor {
namespace kapacitor_udf {

StreamToStreamRequestHandler::StreamToStreamRequestHandler(
    const std::shared_ptr<IUDFAgent>& agent,
    const PointsConverter::PointsToRecordBatchesConversionOptions&
        to_record_batches_options,
    uvw::Loop* loop, const std::chrono::duration<uint64_t>& batch_interval,
    const std::shared_ptr<RecordBatchHandler>& batch_handler)
    : TimerRecordBatchRequestHandlerBase(agent, to_record_batches_options,
                                         loop, batch_interval,
                                         batch_handler) {}

agent::Response StreamToStreamRequestHandler::info() const {
  agent::Response response;
  response.mutable_info()->set_wants(agent::EdgeType::STREAM);
  response.mutable_info()->set_provides(agent::EdgeType::STREAM);
  return response;
}

agent::Response StreamToStreamRequestHandler::init(
    const agent::InitRequest& init_request) {
  agent::Response response;
  response.mutable_init()->set_success(true);
  return response;
}

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
