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
    : public StreamRecordBatchRequestHandlerBase {
 public:
  StreamToStreamRequestHandler(
      const std::shared_ptr<IUDFAgent>& agent,
      const PointsConverter::PointsToRecordBatchesConversionOptions&
          to_record_batches_options,
      const std::shared_ptr<RecordBatchHandler>& handlers_pipeline,
      uvw::Loop* loop, const std::chrono::duration<uint64_t>& batch_interval);

  [[nodiscard]] agent::Response info() const override;
  [[nodiscard]] agent::Response init(
      const agent::InitRequest& init_request) override;
  [[nodiscard]] agent::Response snapshot() const override;
  [[nodiscard]] agent::Response restore(
      const agent::RestoreRequest& restore_request) override;
  void point(const agent::Point& point) override;

  void stop() override;

 private:
  std::shared_ptr<uvw::TimerHandle> batch_timer_;
  std::chrono::duration<uint64_t> batch_interval_;
};

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
