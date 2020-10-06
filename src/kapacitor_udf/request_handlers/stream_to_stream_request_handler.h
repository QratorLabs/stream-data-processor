#pragma once

#include <chrono>
#include <memory>

#include <uvw.hpp>

#include "kapacitor_udf/udf_agent.h"
#include "record_batch_handlers/record_batch_handler.h"
#include "record_batch_request_handler.h"
#include "utils/data_converter.h"

class StreamToStreamRequestHandler : public RecordBatchRequestHandler {
 public:
  StreamToStreamRequestHandler(const std::shared_ptr<IUDFAgent> &agent,
                               const DataConverter::PointsToRecordBatchesConversionOptions &to_record_batches_options,
                               const DataConverter::RecordBatchesToPointsConversionOptions &to_points_options,
                               const std::shared_ptr<RecordBatchHandler> &handlers_pipeline,
                               uvw::Loop *loop,
                               const std::chrono::duration<uint64_t> &batch_interval);

  [[nodiscard]] agent::Response info() const override;
  [[nodiscard]] agent::Response init(const agent::InitRequest& init_request) override;
  [[nodiscard]] agent::Response snapshot() const override;
  [[nodiscard]] agent::Response restore(const agent::RestoreRequest& restore_request) override;
  void beginBatch(const agent::BeginBatch& batch) override;
  void point(const agent::Point& point) override;
  void endBatch(const agent::EndBatch& batch) override;

  void stop() override;

 private:
  std::shared_ptr<uvw::TimerHandle> batch_timer_;
  std::chrono::duration<uint64_t> batch_interval_;
};
