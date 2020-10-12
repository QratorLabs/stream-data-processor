#pragma once

#include <utility>
#include <vector>

#include "kapacitor_udf/udf_agent.h"
#include "record_batch_handlers/record_batch_handler.h"
#include "request_handler.h"
#include "utils/data_converter.h"

#include "udf.pb.h"

class RecordBatchRequestHandler : public RequestHandler {
 public:
  RecordBatchRequestHandler(
      const std::shared_ptr<IUDFAgent>& agent,
      DataConverter::PointsToRecordBatchesConversionOptions
          to_record_batches_options,
      DataConverter::RecordBatchesToPointsConversionOptions to_points_options,
      const std::shared_ptr<RecordBatchHandler>& handler = nullptr);

 protected:
  void handleBatch();

 protected:
  std::shared_ptr<RecordBatchHandler> handler_;
  DataConverter::PointsToRecordBatchesConversionOptions
      to_record_batches_options_;
  DataConverter::RecordBatchesToPointsConversionOptions to_points_options_;
  agent::PointBatch batch_points_;
};
