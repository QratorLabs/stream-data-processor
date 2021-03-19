#pragma once

#include <chrono>
#include <memory>
#include <string>

#include "kapacitor_udf/request_handlers/record_batch_request_handler.h"
#include "kapacitor_udf/utils/points_converter.h"

#include <uvw.hpp>

#include "udf.pb.h"

namespace stream_data_processor {
namespace kapacitor_udf {

using convert_utils::BasePointsConverter;

class StreamAggregateRequestHandler
    : public TimerRecordBatchRequestHandlerBase {
 public:
  explicit StreamAggregateRequestHandler(const IUDFAgent* agent,
                                         uvw::Loop* loop);

  [[nodiscard]] agent::Response info() const override;
  [[nodiscard]] agent::Response init(
      const agent::InitRequest& init_request) override;

 private:
  static const BasePointsConverter::PointsToRecordBatchesConversionOptions
      DEFAULT_TO_RECORD_BATCHES_OPTIONS;
};

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
