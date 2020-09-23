#pragma once

#include <utility>
#include <vector>

#include "kapacitor_udf/udf_agent.h"
#include "record_batch_handlers/record_batch_handler.h"
#include "request_handler.h"
#include "utils/data_converter.h"

#include "udf.pb.h"

class PipelineRequestHandler : public RequestHandler {
 public:
  template <typename U>
  PipelineRequestHandler(const std::shared_ptr<IUDFAgent>& agent, U&& handlers_pipeline,
                         DataConverter::PointsToRecordBatchesConversionOptions to_record_batches_options,
                         DataConverter::RecordBatchesToPointsConversionOptions to_points_options)
      : RequestHandler(agent)
      , handlers_pipeline_(std::forward<U>(handlers_pipeline))
      , to_record_batches_options_(std::move(to_record_batches_options))
      , to_points_options_(std::move(to_points_options)) {

  }

 protected:
  void handleBatch();

 protected:
  std::vector<std::shared_ptr<RecordBatchHandler>> handlers_pipeline_;
  DataConverter::PointsToRecordBatchesConversionOptions to_record_batches_options_;
  DataConverter::RecordBatchesToPointsConversionOptions to_points_options_;
  agent::PointBatch batch_points_;
};


