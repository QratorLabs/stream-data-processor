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
  template <typename HandlerVectorType>
  BatchToStreamRequestHandler(const std::shared_ptr<IUDFAgent>& agent, HandlerVectorType&& handlers_pipeline,
                              const DataConverter::PointsToRecordBatchesConversionOptions& to_record_batches_options,
                              const DataConverter::RecordBatchesToPointsConversionOptions& to_points_options)
      : PipelineRequestHandler(agent, std::forward<HandlerVectorType>(handlers_pipeline),
          to_record_batches_options, to_points_options) {

  }

  [[nodiscard]] agent::Response info() const override;
  [[nodiscard]] agent::Response init(const agent::InitRequest& init_request) override;
  [[nodiscard]] agent::Response snapshot() const override;
  [[nodiscard]] agent::Response restore(const agent::RestoreRequest& restore_request) override;
  void beginBatch(const agent::BeginBatch& batch) override;
  void point(const agent::Point& point) override;
  void endBatch(const agent::EndBatch& batch) override;

 private:
  bool in_batch_{false};
};
