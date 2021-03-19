#include "request_handler.h"

namespace stream_data_processor {
namespace kapacitor_udf {

void StreamRequestHandlerBase::beginBatch(const agent::BeginBatch& batch) {
  agent::Response response;
  response.mutable_error()->set_error(
      "Invalid BeginBatch request, UDF wants stream data");
  getAgent()->writeResponse(response);
}

void StreamRequestHandlerBase::endBatch(const agent::EndBatch& batch) {
  agent::Response response;
  response.mutable_error()->set_error(
      "Invalid EndBatch request, UDF wants stream data");
  getAgent()->writeResponse(response);
}

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
