#pragma once

#include <string>
#include <unordered_map>

#include "record_batch_request_handler.h"
#include "utils/data_converter.h"

#include "udf.pb.h"

class BatchAggregateRequestHandler : public RecordBatchRequestHandler {
 public:
  explicit BatchAggregateRequestHandler(const std::shared_ptr<IUDFAgent>& agent);

  [[nodiscard]] agent::Response info() const override;
  [[nodiscard]] agent::Response init(const agent::InitRequest& init_request) override;
  [[nodiscard]] agent::Response snapshot() const override;
  [[nodiscard]] agent::Response restore(const agent::RestoreRequest& restore_request) override;
  void beginBatch(const agent::BeginBatch& batch) override;
  void point(const agent::Point& point) override;
  void endBatch(const agent::EndBatch& batch) override;

 private:
  static const DataConverter::PointsToRecordBatchesConversionOptions TO_RECORD_BATCHES_OPTIONS;
  static const DataConverter::RecordBatchesToPointsConversionOptions TO_POINTS_OPTIONS;

  bool in_batch_{false};
};


