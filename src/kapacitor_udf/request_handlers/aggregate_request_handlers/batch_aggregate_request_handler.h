#pragma once

#include <memory>

#include "kapacitor_udf/request_handlers/record_batch_request_handler.h"
#include "kapacitor_udf/utils/points_converter.h"

#include "udf.pb.h"

namespace stream_data_processor {
namespace kapacitor_udf {

class BatchAggregateRequestHandler : public RecordBatchRequestHandler {
 public:
  explicit BatchAggregateRequestHandler(
      const std::shared_ptr<IUDFAgent>& agent);

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
  static const BasePointsConverter::PointsToRecordBatchesConversionOptions
      DEFAULT_TO_RECORD_BATCHES_OPTIONS;

  bool in_batch_{false};
};

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
