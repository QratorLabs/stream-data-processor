#pragma once

#include <chrono>
#include <memory>
#include <string>

#include "kapacitor_udf/request_handlers/record_batch_request_handler.h"
#include "kapacitor_udf/utils/points_converter.h"

#include "udf.pb.h"

namespace stream_data_processor {
namespace kapacitor_udf {

class StreamAggregateRequestHandler
    : public StreamRecordBatchRequestHandlerBase {
 public:
  explicit StreamAggregateRequestHandler(
      const std::shared_ptr<IUDFAgent>& agent);

  [[nodiscard]] agent::Response info() const override;
  [[nodiscard]] agent::Response init(
      const agent::InitRequest& init_request) override;
  void point(const agent::Point& point) override;

 private:
  void parseToleranceOption(
      const google::protobuf::RepeatedPtrField<agent::Option>&
          request_options);

  bool needHandleBefore(const agent::Point& point) const;

 private:
  static const PointsConverter::PointsToRecordBatchesConversionOptions
      DEFAULT_TO_RECORD_BATCHES_OPTIONS;

  static const std::string TOLERANCE_OPTION_NAME;

  std::chrono::nanoseconds tolerance_{0};
};

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
