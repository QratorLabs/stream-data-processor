#pragma once

#include <chrono>
#include <memory>

#include <uvw.hpp>

#include "kapacitor_udf/udf_agent.h"
#include "kapacitor_udf/utils/points_converter.h"
#include "record_batch_handlers/record_batch_handler.h"
#include "record_batch_request_handler.h"

namespace stream_data_processor {
namespace kapacitor_udf {

class StreamToStreamRequestHandler
    : public TimerRecordBatchRequestHandlerBase {
 public:
  StreamToStreamRequestHandler(
      const std::shared_ptr<IUDFAgent>& agent, uvw::Loop* loop,
      const std::chrono::duration<uint64_t>& batch_interval);

  [[nodiscard]] agent::Response info() const override;
  [[nodiscard]] agent::Response init(
      const agent::InitRequest& init_request) override;
};

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
