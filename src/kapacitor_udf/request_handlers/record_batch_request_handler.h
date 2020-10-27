#pragma once

#include <utility>
#include <vector>

#include "kapacitor_udf/udf_agent.h"
#include "record_batch_handlers/record_batch_handler.h"
#include "request_handler.h"
#include "kapacitor_udf/points_converter.h"

#include "udf.pb.h"

class RecordBatchRequestHandler : public RequestHandler {
 public:
  RecordBatchRequestHandler(
      const std::shared_ptr<IUDFAgent>& agent,
      PointsConverter::PointsToRecordBatchesConversionOptions
          to_record_batches_options,
      PointsConverter::RecordBatchesToPointsConversionOptions to_points_options,
      const std::shared_ptr<RecordBatchHandler>& handler = nullptr);

 protected:
  void handleBatch();

 protected:
  std::shared_ptr<RecordBatchHandler> handler_;
  PointsConverter::PointsToRecordBatchesConversionOptions
      to_record_batches_options_;
  PointsConverter::RecordBatchesToPointsConversionOptions to_points_options_;
  agent::PointBatch batch_points_;
};
