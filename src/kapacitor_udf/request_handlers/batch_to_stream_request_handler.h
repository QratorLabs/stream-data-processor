#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "kapacitor_udf/udf_agent.h"
#include "pipeline_request_handler.h"
#include "record_batch_handlers/record_batch_handler.h"
#include "utils/data_converter.h"

#include "udf.pb.h"

class BatchToStreamRequestHandler : public PipelineRequestHandler {
 public:
  template <typename U>
  BatchToStreamRequestHandler(const std::shared_ptr<IUDFAgent>& agent, U&& handlers_pipeline,
                              const DataConverter::PointsToRecordBatchesConversionOptions& to_record_batches_options,
                              const DataConverter::RecordBatchesToPointsConversionOptions& to_points_options)
      : PipelineRequestHandler(agent, std::forward<U>(handlers_pipeline), to_record_batches_options, to_points_options) {

  }

  [[nodiscard]] agent::Response info() const override;
  agent::Response init(const agent::InitRequest& init_request) override;
  agent::Response snapshot() override;
  agent::Response restore(const agent::RestoreRequest& restore_request) override;
  void beginBatch(const agent::BeginBatch& batch) override;
  void point(const agent::Point& point) override;
  void endBatch(const agent::EndBatch& batch) override;

 private:
  bool in_batch_{false};
};


