#pragma once

#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "kapacitor_udf/utils/points_converter.h"
#include "record_batch_handlers/stateful_handlers/window_handler.h"
#include "record_batch_request_handler.h"
#include "utils/time_utils.h"

namespace stream_data_processor {
namespace kapacitor_udf {

using convert_utils::BasePointsConverter;

namespace internal {

using convert_utils::BasePointsConverterDecorator;

class WindowOptionsConverterDecorator : public BasePointsConverterDecorator {
 public:
  struct WindowOptions {
    std::optional<std::pair<std::string, time_utils::TimeUnit>> period_option;
    std::optional<std::pair<std::string, time_utils::TimeUnit>> every_option;
  };

 public:
  template <class OptionsType>
  WindowOptionsConverterDecorator(std::shared_ptr<PointsConverter> wrappee,
                                  OptionsType&& options)
      : BasePointsConverterDecorator(std::move(wrappee)),
        options_(std::forward<OptionsType>(options)) {}

  arrow::Result<arrow::RecordBatchVector> convertToRecordBatches(
      const agent::PointBatch& points) const override;

 private:
  WindowOptions options_;
};

struct WindowOptions {
  internal::WindowOptionsConverterDecorator::WindowOptions convert_options;
  WindowHandler::WindowOptions window_handler_options;
  std::chrono::seconds emit_timeout;
};

google::protobuf::Map<std::string, agent::OptionInfo> getWindowOptionsMap();

WindowOptions parseWindowOptions(
    const google::protobuf::RepeatedPtrField<agent::Option>& request_options);

}  // namespace internal

class DynamicWindowRequestHandler
    : public TimerRecordBatchRequestHandlerBase {
 public:
  DynamicWindowRequestHandler(const IUDFAgent* agent, uvw::Loop* loop);

  [[nodiscard]] agent::Response info() const override;
  [[nodiscard]] agent::Response init(
      const agent::InitRequest& init_request) override;

 private:
  static const BasePointsConverter::PointsToRecordBatchesConversionOptions
      DEFAULT_TO_RECORD_BATCHES_OPTIONS;
};

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
