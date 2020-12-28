#pragma once

#include <chrono>
#include <utility>
#include <vector>

#include <uvw.hpp>

#include "kapacitor_udf/udf_agent.h"
#include "kapacitor_udf/utils/points_converter.h"
#include "record_batch_handlers/record_batch_handler.h"
#include "request_handler.h"

#include "udf.pb.h"

namespace stream_data_processor {
namespace kapacitor_udf {

using convert_utils::BasePointsConverter;

class RecordBatchRequestHandler : public RequestHandler {
 public:
  explicit RecordBatchRequestHandler(const std::shared_ptr<IUDFAgent>& agent);

  template <class HandlerType>
  void setHandler(HandlerType&& handler) {
    handler_ = std::forward<HandlerType>(handler);
  }

  template <class ConverterType>
  void setPointsConverter(ConverterType&& converter) {
    points_converter_ = std::forward<ConverterType>(converter);
  }

 protected:
  void handleBatch();
  const agent::PointBatch& getPoints() const;
  void restorePointsFromSnapshotData(const std::string& data);
  void addPoint(const agent::Point& point);
  void setPointsName(const std::string& name);

 private:
  std::shared_ptr<RecordBatchHandler> handler_{nullptr};
  std::shared_ptr<convert_utils::PointsConverter> points_converter_{nullptr};
  agent::PointBatch batch_points_;
};

class StreamRecordBatchRequestHandlerBase : public RecordBatchRequestHandler {
 public:
  explicit StreamRecordBatchRequestHandlerBase(
      const std::shared_ptr<IUDFAgent>& agent);

  [[nodiscard]] agent::Response snapshot() const override;
  [[nodiscard]] agent::Response restore(
      const agent::RestoreRequest& restore_request) override;

  void beginBatch(const agent::BeginBatch& batch) override;
  void endBatch(const agent::EndBatch& batch) override;
};

class TimerRecordBatchRequestHandlerBase
    : public StreamRecordBatchRequestHandlerBase {
 public:
  TimerRecordBatchRequestHandlerBase(const std::shared_ptr<IUDFAgent>& agent,
                                     uvw::Loop* loop);

  TimerRecordBatchRequestHandlerBase(
      const std::shared_ptr<IUDFAgent>& agent, uvw::Loop* loop,
      const std::chrono::seconds& batch_interval);

  void point(const agent::Point& point) override;

  void stop() override;

 protected:
  template <typename SecondsType>
  void setEmitTimeout(SecondsType&& new_timeout) {
    emit_timeout_ = std::forward<SecondsType>(new_timeout);
    if (emit_timer_->active()) {
      emit_timer_->repeat(emit_timeout_);
      emit_timer_->again();
    }
  }

 private:
  std::shared_ptr<uvw::TimerHandle> emit_timer_;
  std::chrono::seconds emit_timeout_;
};

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
