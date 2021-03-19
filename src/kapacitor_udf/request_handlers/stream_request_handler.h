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

using storage_utils::IPointsStorage;

class StreamRequestHandler : public TimerRecordBatchRequestHandlerBase {
 public:
  StreamRequestHandler(const IUDFAgent* agent, uvw::Loop* loop,
                       std::chrono::duration<uint64_t> batch_interval,
                       std::unique_ptr<IPointsStorage>&& points_storage);

  [[nodiscard]] agent::Response info() const override;
  [[nodiscard]] agent::Response init(
      const agent::InitRequest& init_request) override;
};

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
