#include <sstream>

#include <spdlog/spdlog.h>

#include "kapacitor_udf/utils/grouping_utils.h"
#include "record_batch_request_handler.h"

namespace stream_data_processor {
namespace kapacitor_udf {

BatchRecordBatchRequestHandlerBase::BatchRecordBatchRequestHandlerBase(
    const IUDFAgent* agent)
    : RequestHandler(agent) {}

agent::Response BatchRecordBatchRequestHandlerBase::snapshot() const {
  agent::Response response;
  if (points_storage_ == nullptr) {
    response.mutable_error()->set_error(
        "Can't provide snapshot. "
        "PointsStorage is not provided");
    spdlog::critical(response.error().error());
    getAgent()->writeResponse(response);

    agent::Response empty_snapshot;
    empty_snapshot.mutable_snapshot()->set_snapshot("");
    return empty_snapshot;
  }

  std::stringstream snapshot_builder;
  if (in_batch_) {
    snapshot_builder.put('1');
  } else {
    snapshot_builder.put('0');
  }

  snapshot_builder << points_storage_->snapshot();
  response.mutable_snapshot()->set_snapshot(snapshot_builder.str());
  return response;
}

agent::Response BatchRecordBatchRequestHandlerBase::restore(
    const agent::RestoreRequest& restore_request) {
  agent::Response response;
  if (points_storage_ == nullptr) {
    response.mutable_restore()->set_success(false);
    response.mutable_restore()->set_error(
        "Can't restore from snapshot. "
        "PointsStorage is not provided");
    spdlog::critical(response.restore().error());
    return response;
  }

  auto restore_snapshot = restore_request.snapshot();
  if (restore_snapshot.empty()) {
    response.mutable_restore()->set_success(false);
    response.mutable_restore()->set_error(
        "Can't restore from empty snapshot");
    spdlog::error(response.restore().error());
    return response;
  }

  if ((restore_snapshot[0] != '0' && restore_snapshot[0] != '1') ||
      !points_storage_->restore(restore_snapshot.substr(1))) {
    response.mutable_restore()->set_success(false);
    response.mutable_restore()->set_error(
        fmt::format("Invalid snapshot: {}", restore_snapshot));
    spdlog::error(response.restore().error());
    return response;
  }

  in_batch_ = restore_snapshot[0] == '1';
  response.mutable_restore()->set_success(true);
  return response;
}

void BatchRecordBatchRequestHandlerBase::beginBatch(
    const agent::BeginBatch& batch) {
  in_batch_ = true;
}

void BatchRecordBatchRequestHandlerBase::point(const agent::Point& point) {
  agent::Response response;
  if (points_storage_ == nullptr) {
    response.mutable_error()->set_error(
        "Can't handle point. "
        "PointsStorage is not provided");
    spdlog::critical(response.error().error());
    getAgent()->writeResponse(response);
    return;
  }

  if (in_batch_) {
    points_storage_->addPoint(point);
  } else {
    response.mutable_error()->set_error("Can't add point: not in batch");
    getAgent()->writeResponse(response);
  }
}

void BatchRecordBatchRequestHandlerBase::endBatch(
    const agent::EndBatch& batch) {
  agent::Response response;
  if (points_storage_ == nullptr) {
    response.mutable_error()->set_error(
        "Can't handle EndBatch message. "
        "PointsStorage is not provided");
    spdlog::critical(response.error().error());
    getAgent()->writeResponse(response);
    return;
  }

  if (!in_batch_) {
    response.mutable_error()->set_error(
        "Can't end batch: currently not in batch");
    getAgent()->writeResponse(response);
  }

  points_storage_->setPointsName(batch.name());
  auto handle_status = points_storage_->handleBatch();

  if (!handle_status.ok()) {
    spdlog::error(handle_status.message());
  }

  points_storage_->clear();
  in_batch_ = false;
}

StreamRecordBatchRequestHandlerBase::StreamRecordBatchRequestHandlerBase(
    const IUDFAgent* agent)
    : StreamRequestHandlerBase(agent) {}

agent::Response StreamRecordBatchRequestHandlerBase::snapshot() const {
  agent::Response response;
  if (points_storage_ == nullptr) {
    response.mutable_error()->set_error(
        "Can't provide snapshot. "
        "PointsStorage is not provided");
    spdlog::critical(response.error().error());
    getAgent()->writeResponse(response);

    agent::Response empty_snapshot;
    empty_snapshot.mutable_snapshot()->set_snapshot("");
    return empty_snapshot;
  }

  response.mutable_snapshot()->set_snapshot(points_storage_->snapshot());
  return response;
}

agent::Response StreamRecordBatchRequestHandlerBase::restore(
    const agent::RestoreRequest& restore_request) {
  agent::Response response;
  if (points_storage_ == nullptr) {
    response.mutable_restore()->set_success(false);
    response.mutable_restore()->set_error(
        "Can't restore from snapshot. "
        "PointsStorage is not provided");
    spdlog::critical(response.restore().error());
    return response;
  }

  if (!points_storage_->restore(restore_request.snapshot())) {
    response.mutable_restore()->set_success(false);
    response.mutable_restore()->set_error(fmt::format(
        "Can't restore from snapshot: {}", restore_request.snapshot()));
    return response;
  }

  response.mutable_restore()->set_success(true);
  return response;
}

void StreamRecordBatchRequestHandlerBase::handleBatch() const {
  if (getPointsStorage() == nullptr) {
    agent::Response error_response;
    error_response.mutable_error()->set_error(
        "Can't handle batch. "
        "PointsStorage is not provided");
    spdlog::critical(error_response.error().error());
    getAgent()->writeResponse(error_response);
    return;
  }

  auto handle_status = getPointsStorage()->handleBatch();

  if (!handle_status.ok()) {
    spdlog::error(handle_status.message());
  }

  getPointsStorage()->clear();
}

TimerRecordBatchRequestHandlerBase::TimerRecordBatchRequestHandlerBase(
    const IUDFAgent* agent, uvw::Loop* loop)
    : StreamRecordBatchRequestHandlerBase(agent),
      emit_timer_(loop->resource<uvw::TimerHandle>()) {
  emit_timer_->on<uvw::TimerEvent>(emit_callback_);
}

TimerRecordBatchRequestHandlerBase::TimerRecordBatchRequestHandlerBase(
    const IUDFAgent* agent, uvw::Loop* loop,
    std::chrono::seconds batch_interval)
    : StreamRecordBatchRequestHandlerBase(agent),
      emit_timer_(loop->resource<uvw::TimerHandle>()),
      emit_timeout_(batch_interval) {
  emit_timer_->on<uvw::TimerEvent>(emit_callback_);
}

void TimerRecordBatchRequestHandlerBase::point(const agent::Point& point) {
  if (getPointsStorage() == nullptr) {
    agent::Response error_response;
    error_response.mutable_error()->set_error(
        "Can't handle point. PointsStorage is not provided");
    spdlog::critical(error_response.error().error());
    getAgent()->writeResponse(error_response);
    return;
  }

  getPointsStorage()->addPoint(point);
  if (!emit_timer_->active()) {
    emit_timer_->start(emit_timeout_, emit_timeout_);
  }
}

void TimerRecordBatchRequestHandlerBase::stop() {
  if (getPointsStorage() != nullptr) {
    handleBatch();
  }

  emit_timer_->stop();
  RequestHandler::stop();
}

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
