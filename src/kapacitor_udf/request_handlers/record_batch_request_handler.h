#pragma once

#include <chrono>
#include <utility>
#include <vector>

#include <uvw.hpp>

#include "kapacitor_udf/udf_agent.h"
#include "kapacitor_udf/utils/points_converter.h"
#include "metadata/metadata.h"
#include "record_batch_handlers/record_batch_handler.h"
#include "request_handler.h"

#include "udf.pb.h"

namespace stream_data_processor {
namespace kapacitor_udf {

using convert_utils::BasePointsConverter;

class RecordBatchRequestHandler : public RequestHandler {
 public:
  RecordBatchRequestHandler(const std::shared_ptr<IUDFAgent>& agent,
                            bool provides_batch);

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
  static arrow::Result<agent::BeginBatch> getBeginBatchResponse(
      const arrow::RecordBatch& record_batch);

  static arrow::Result<agent::EndBatch> getEndBatchResponse(
      const arrow::RecordBatch& record_batch);

  static arrow::Result<std::string> getGroupString(
      const arrow::RecordBatch& record_batch);

  template <class BatchResponseType>
  static arrow::Status setGroupTagsAndByName(
      BatchResponseType* batch_response,
      const arrow::RecordBatch& record_batch) {
    std::string measurement_column_name;
    ARROW_ASSIGN_OR_RAISE(
        measurement_column_name,
        metadata::getMeasurementColumnNameMetadata(record_batch));

    auto group = metadata::extractGroup(record_batch);

    batch_response->set_byname(false);
    for (size_t i = 0; i < group.group_columns_values_size(); ++i) {
      auto& group_key = group.group_columns_names().columns_names(i);
      if (group_key != measurement_column_name) {
        auto& group_value = group.group_columns_values(i);
        batch_response->mutable_tags()->operator[](group_key) = group_value;
      } else {
        batch_response->set_byname(true);
      }
    }

    return arrow::Status::OK();
  }

  static arrow::Result<int64_t> getTMax(
      const arrow::RecordBatch& record_batch);

 private:
  bool provides_batch_;
  std::shared_ptr<RecordBatchHandler> handler_{nullptr};
  std::shared_ptr<convert_utils::PointsConverter> points_converter_{nullptr};
  agent::PointBatch batch_points_;
};

class StreamRecordBatchRequestHandlerBase : public RecordBatchRequestHandler {
 public:
  explicit StreamRecordBatchRequestHandlerBase(
      const std::shared_ptr<IUDFAgent>& agent, bool provides_batch);

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
                                     bool provides_batch, uvw::Loop* loop);

  TimerRecordBatchRequestHandlerBase(
      const std::shared_ptr<IUDFAgent>& agent, bool provides_batch,
      uvw::Loop* loop, const std::chrono::seconds& batch_interval);

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
