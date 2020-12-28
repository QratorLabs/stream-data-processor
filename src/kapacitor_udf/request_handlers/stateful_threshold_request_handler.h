#pragma once

#include "record_batch_handlers/stateful_handlers/threshold_state_machine.h"
#include "record_batch_request_handler.h"

#include "udf.pb.h"

namespace stream_data_processor {
namespace kapacitor_udf {

class StatefulThresholdRequestHandler
    : public StreamRecordBatchRequestHandlerBase {
 public:
  explicit StatefulThresholdRequestHandler(
      const std::shared_ptr<IUDFAgent>& agent);

  [[nodiscard]] agent::Response info() const override;
  [[nodiscard]] agent::Response init(
      const agent::InitRequest& init_request) override;
  void point(const agent::Point& point) override;

 private:
  static const BasePointsConverter::PointsToRecordBatchesConversionOptions
      DEFAULT_TO_RECORD_BATCHES_OPTIONS;
};

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
