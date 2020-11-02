#pragma once

#include <utility>
#include <vector>

#include "kapacitor_udf/points_converter.h"
#include "kapacitor_udf/udf_agent.h"
#include "record_batch_handlers/record_batch_handler.h"
#include "request_handler.h"

#include "udf.pb.h"

class RecordBatchRequestHandler : public RequestHandler {
 public:
  RecordBatchRequestHandler(
      const std::shared_ptr<IUDFAgent>& agent,
      PointsConverter::PointsToRecordBatchesConversionOptions
          to_record_batches_options,
      const std::shared_ptr<RecordBatchHandler>& handler = nullptr);

 protected:
  void handleBatch();

 protected:
  std::shared_ptr<RecordBatchHandler> handler_;
  PointsConverter::PointsToRecordBatchesConversionOptions
      to_record_batches_options_;
  agent::PointBatch batch_points_;
};

class StreamRecordBatchRequestHandlerBase : public RecordBatchRequestHandler {
 public:
  StreamRecordBatchRequestHandlerBase(
      const std::shared_ptr<IUDFAgent>& agent,
      PointsConverter::PointsToRecordBatchesConversionOptions
          to_record_batches_options,
      const std::shared_ptr<RecordBatchHandler>& handler = nullptr);

  void beginBatch(const agent::BeginBatch& batch) override;
  void endBatch(const agent::EndBatch& batch) override;
};
