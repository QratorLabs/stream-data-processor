#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "kapacitor_udf/udf_agent.h"
#include "record_batch_handlers/record_batch_handler.h"
#include "record_batch_request_handler.h"
#include "kapacitor_udf/points_converter.h"

#include "udf.pb.h"

class BatchToStreamRequestHandler : public RecordBatchRequestHandler {
 public:
  BatchToStreamRequestHandler(
      const std::shared_ptr<IUDFAgent>& agent,
      const PointsConverter::PointsToRecordBatchesConversionOptions&
          to_record_batches_options,
      const PointsConverter::RecordBatchesToPointsConversionOptions&
          to_points_options,
      const std::shared_ptr<RecordBatchHandler>& handler);

  [[nodiscard]] agent::Response info() const override;
  [[nodiscard]] agent::Response init(
      const agent::InitRequest& init_request) override;
  [[nodiscard]] agent::Response snapshot() const override;
  [[nodiscard]] agent::Response restore(
      const agent::RestoreRequest& restore_request) override;
  void beginBatch(const agent::BeginBatch& batch) override;
  void point(const agent::Point& point) override;
  void endBatch(const agent::EndBatch& batch) override;

 private:
  bool in_batch_{false};
};
