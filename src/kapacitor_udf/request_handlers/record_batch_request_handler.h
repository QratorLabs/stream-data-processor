#pragma once

#include <chrono>
#include <functional>
#include <optional>
#include <utility>
#include <vector>

#include <uvw.hpp>

#include "kapacitor_udf/udf_agent.h"
#include "kapacitor_udf/utils/points_converter.h"
#include "kapacitor_udf/utils/points_storage.h"
#include "metadata/metadata.h"
#include "record_batch_handlers/record_batch_handler.h"
#include "request_handler.h"

#include "udf.pb.h"

namespace stream_data_processor {
namespace kapacitor_udf {

using storage_utils::IPointsStorage;

class BatchRecordBatchRequestHandlerBase : public RequestHandler {
 public:
  explicit BatchRecordBatchRequestHandlerBase(const IUDFAgent* agent);

  [[nodiscard]] agent::Response snapshot() const override;
  [[nodiscard]] agent::Response restore(
      const agent::RestoreRequest& restore_request) override;

  void beginBatch(const agent::BeginBatch& batch) override;
  void point(const agent::Point& point) override;
  void endBatch(const agent::EndBatch& batch) override;

 protected:
  void setPointsStorage(std::unique_ptr<IPointsStorage>&& points_storage) {
    points_storage_ = std::move(points_storage);
  }

  IPointsStorage* getPointsStorage() const { return points_storage_.get(); }

 private:
  std::unique_ptr<IPointsStorage> points_storage_{nullptr};
  bool in_batch_{false};
};

class StreamRecordBatchRequestHandlerBase : public StreamRequestHandlerBase {
 public:
  explicit StreamRecordBatchRequestHandlerBase(const IUDFAgent* agent);

  [[nodiscard]] agent::Response snapshot() const override;
  [[nodiscard]] agent::Response restore(
      const agent::RestoreRequest& restore_request) override;

 protected:
  void setPointsStorage(std::unique_ptr<IPointsStorage>&& points_storage) {
    points_storage_ = std::move(points_storage);
  }

  IPointsStorage* getPointsStorage() const { return points_storage_.get(); }

  void handleBatch() const;

 private:
  std::unique_ptr<IPointsStorage> points_storage_{nullptr};
};

class TimerRecordBatchRequestHandlerBase
    : public StreamRecordBatchRequestHandlerBase {
 public:
  TimerRecordBatchRequestHandlerBase(const IUDFAgent* agent, uvw::Loop* loop);

  TimerRecordBatchRequestHandlerBase(const IUDFAgent* agent, uvw::Loop* loop,
                                     std::chrono::seconds batch_interval);

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

  const std::function<void(const uvw::TimerEvent&, const uvw::TimerHandle&)>
      emit_callback_ =
          [this](const uvw::TimerEvent& /* non-used */,
                 const uvw::TimerHandle& /* non-used */) { handleBatch(); };
};

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
