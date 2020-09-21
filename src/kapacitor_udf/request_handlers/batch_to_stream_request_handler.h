#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "kapacitor_udf/agent.h"
#include "request_handler.h"
#include "record_batch_handlers/record_batch_handler.h"
#include "utils/data_converter.h"

#include "udf.pb.h"

class BatchToStreamRequestHandler : public RequestHandler {
 public:
  template <typename U>
  BatchToStreamRequestHandler(const std::shared_ptr<IAgent>& agent, U&& handlers_pipeline,
                              DataConverter::PointsToRecordBatchesConversionOptions to_record_batches_options,
                              DataConverter::RecordBatchesToPointsConversionOptions to_points_options)
      : agent_(agent)
      , handlers_pipeline_(std::forward<U>(handlers_pipeline))
      , to_record_batches_options_(std::move(to_record_batches_options))
      , to_points_options_((std::move(to_points_options))) {

  }

  [[nodiscard]] agent::Response info() const override;
  agent::Response init(const agent::InitRequest& init_request) override;
  agent::Response snapshot() override;
  agent::Response restore(const agent::RestoreRequest& restore_request) override;
  void beginBatch(const agent::BeginBatch& batch) override;
  void point(const agent::Point& point) override;
  void endBatch(const agent::EndBatch& batch) override;

 private:
  std::weak_ptr<IAgent> agent_;
  std::vector<std::shared_ptr<RecordBatchHandler>> handlers_pipeline_;
  DataConverter::PointsToRecordBatchesConversionOptions to_record_batches_options_;
  DataConverter::RecordBatchesToPointsConversionOptions to_points_options_;
  bool in_batch_{false};
  agent::PointBatch batch_points_;
};


