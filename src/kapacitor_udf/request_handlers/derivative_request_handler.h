#pragma once

#include "record_batch_handlers/stateful_handlers/derivative_handler.h"
#include "record_batch_request_handler.h"

namespace stream_data_processor {
namespace kapacitor_udf {

namespace internal {

struct DerivativeOptions {
  DerivativeHandler::DerivativeOptions options;
  std::chrono::seconds emit_timeout;
};

google::protobuf::Map<std::string, agent::OptionInfo>
getDerivativeOptionsMap();

DerivativeOptions parseDerivativeOptions(
    const google::protobuf::RepeatedPtrField<agent::Option>& request_options);

}  // namespace internal

using convert_utils::BasePointsConverter;

class DerivativeRequestHandler : public TimerRecordBatchRequestHandlerBase {
 public:
  DerivativeRequestHandler(const IUDFAgent* agent, uvw::Loop* loop);

  [[nodiscard]] agent::Response info() const override;
  [[nodiscard]] agent::Response init(
      const agent::InitRequest& init_request) override;

 private:
  static const BasePointsConverter::PointsToRecordBatchesConversionOptions
      DEFAULT_TO_RECORD_BATCHES_OPTIONS;
};

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
