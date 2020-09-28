#pragma once

#include <chrono>
#include <memory>

#include <uvw.hpp>

#include "kapacitor_udf/udf_agent.h"
#include "record_batch_handlers/record_batch_handler.h"
#include "pipeline_request_handler.h"
#include "utils/data_converter.h"

class StreamToStreamRequestHandler : public PipelineRequestHandler {
 public:
  template <typename U>
  StreamToStreamRequestHandler(const std::shared_ptr<IUDFAgent>& agent, U&& handlers_pipeline,
                              const DataConverter::PointsToRecordBatchesConversionOptions& to_record_batches_options,
                              const DataConverter::RecordBatchesToPointsConversionOptions& to_points_options,
                              const std::shared_ptr<uvw::Loop>& loop,
                              const std::chrono::duration<uint64_t>& batch_interval = std::chrono::duration<uint64_t>(10))
      : PipelineRequestHandler(agent, std::forward<U>(handlers_pipeline), to_record_batches_options, to_points_options)
      , batch_timer_(loop->resource<uvw::TimerHandle>())
      , batch_interval_(batch_interval) {
    batch_timer_->on<uvw::TimerEvent>([this](const uvw::TimerEvent& event, uvw::TimerHandle& handle) {
      handleBatch();
    });
  }

  [[nodiscard]] agent::Response info() const override;
  [[nodiscard]] agent::Response init(const agent::InitRequest& init_request) override;
  [[nodiscard]] agent::Response snapshot() override;
  [[nodiscard]] agent::Response restore(const agent::RestoreRequest& restore_request) override;
  void beginBatch(const agent::BeginBatch& batch) override;
  void point(const agent::Point& point) override;
  void endBatch(const agent::EndBatch& batch) override;

  void stop() override;

 private:
  std::shared_ptr<uvw::TimerHandle> batch_timer_;
  std::chrono::duration<uint64_t> batch_interval_;
};


