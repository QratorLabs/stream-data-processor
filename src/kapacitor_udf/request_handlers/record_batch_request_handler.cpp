#include <spdlog/spdlog.h>

#include "kapacitor_udf/utils/grouping_utils.h"
#include "metadata/metadata.h"
#include "record_batch_handlers/aggregate_functions/aggregate_functions.h"
#include "record_batch_request_handler.h"
#include "utils/arrow_utils.h"

namespace stream_data_processor {
namespace kapacitor_udf {

using convert_utils::BasePointsConverter;

RecordBatchRequestHandler::RecordBatchRequestHandler(
    const std::shared_ptr<IUDFAgent>& agent, bool provides_batch)
    : RequestHandler(agent), provides_batch_(provides_batch) {}

void RecordBatchRequestHandler::handleBatch() {
  spdlog::debug("Handling batch...");
  agent::Response response;

  if (handler_ == nullptr) {
    response.mutable_error()->set_error("RecordBatchHandler is not provided");
    spdlog::critical(response.error().error());
    getAgent()->writeResponse(response);
    return;
  }

  if (points_converter_ == nullptr) {
    response.mutable_error()->set_error("PointsConverter is not provided");
    spdlog::critical(response.error().error());
    getAgent()->writeResponse(response);
    return;
  }

  if (batch_points_.points_size() == 0) {
    spdlog::debug("No points available, waiting for new points...");
    return;
  }

  auto record_batches =
      points_converter_->convertToRecordBatches(batch_points_);

  batch_points_.mutable_points()->Clear();

  if (!record_batches.ok()) {
    spdlog::error(record_batches.status().message());
    return;
  }

  auto result = handler_->handle(record_batches.ValueOrDie());
  if (!result.ok()) {
    spdlog::error(result.status().message());
    return;
  }

  auto result_batches = result.ValueOrDie();
  for (auto& result_batch : result_batches) {
    auto response_points_result =
        points_converter_->convertToPoints({result_batch});

    if (!response_points_result.ok()) {
      spdlog::error(response_points_result.status().message());
      continue;
    }

    auto response_points = response_points_result.ValueOrDie();

    arrow::Result<agent::BeginBatch> begin_result;
    arrow::Result<agent::EndBatch> end_result;

    if (provides_batch_) {
      begin_result = getBeginBatchResponse(*result_batch);
      if (!begin_result.ok()) {
        spdlog::error(begin_result.status().message());
        continue;
      }

      end_result = getEndBatchResponse(*result_batch);
      if (!end_result.ok()) {
        spdlog::error(end_result.status().message());
        continue;
      }

      response.mutable_begin()->CopyFrom(begin_result.ValueOrDie());
      getAgent()->writeResponse(response);
    }

    for (auto& point : response_points.points()) {
      response.mutable_point()->CopyFrom(point);
      getAgent()->writeResponse(response);
    }

    if (provides_batch_) {
      response.mutable_end()->CopyFrom(end_result.ValueOrDie());
      getAgent()->writeResponse(response);
    }
  }
}

const agent::PointBatch& RecordBatchRequestHandler::getPoints() const {
  return batch_points_;
}

void RecordBatchRequestHandler::restorePointsFromSnapshotData(
    const std::string& data) {
  batch_points_.mutable_points()->Clear();
  batch_points_.ParseFromString(data);
}

void RecordBatchRequestHandler::addPoint(const agent::Point& point) {
  auto new_point = batch_points_.mutable_points()->Add();
  new_point->CopyFrom(point);
}

void RecordBatchRequestHandler::setPointsName(const std::string& name) {
  for (auto& point : *batch_points_.mutable_points()) {
    point.set_name(name);
  }
}

arrow::Result<agent::BeginBatch>
RecordBatchRequestHandler::getBeginBatchResponse(
    const arrow::RecordBatch& record_batch) {
  agent::BeginBatch begin_batch_response;
  ARROW_ASSIGN_OR_RAISE(*begin_batch_response.mutable_name(),
                        metadata::getMeasurementAndValidate(record_batch));

  ARROW_ASSIGN_OR_RAISE(*begin_batch_response.mutable_group(),
                        getGroupString(record_batch));

  ARROW_RETURN_NOT_OK(
      setGroupTagsAndByName(&begin_batch_response, record_batch));

  begin_batch_response.set_size(record_batch.num_rows());

  return begin_batch_response;
}

arrow::Result<agent::EndBatch> RecordBatchRequestHandler::getEndBatchResponse(
    const arrow::RecordBatch& record_batch) {
  agent::EndBatch end_batch_response;
  ARROW_ASSIGN_OR_RAISE(*end_batch_response.mutable_name(),
                        metadata::getMeasurementAndValidate(record_batch));

  ARROW_ASSIGN_OR_RAISE(*end_batch_response.mutable_group(),
                        getGroupString(record_batch));

  ARROW_RETURN_NOT_OK(
      setGroupTagsAndByName(&end_batch_response, record_batch));

  int64_t tmax_value;
  ARROW_ASSIGN_OR_RAISE(tmax_value, getTMax(record_batch));
  end_batch_response.set_tmax(tmax_value);

  return end_batch_response;
}

arrow::Result<std::string> RecordBatchRequestHandler::getGroupString(
    const arrow::RecordBatch& record_batch) {
  std::string measurement_column_name;
  ARROW_ASSIGN_OR_RAISE(
      measurement_column_name,
      metadata::getMeasurementColumnNameMetadata(record_batch));

  std::unordered_map<std::string, metadata::ColumnType> column_types;
  ARROW_ASSIGN_OR_RAISE(column_types, metadata::getColumnTypes(record_batch));

  auto group = metadata::extractGroup(record_batch);
  return grouping_utils::encode(group, measurement_column_name, column_types);
}

arrow::Result<int64_t> RecordBatchRequestHandler::getTMax(
    const arrow::RecordBatch& record_batch) {
  std::string time_column_name;
  ARROW_ASSIGN_OR_RAISE(time_column_name,
                        metadata::getTimeColumnNameMetadata(record_batch));

  std::shared_ptr<arrow::Scalar> tmax_scalar;
  ARROW_ASSIGN_OR_RAISE(tmax_scalar, LastAggregateFunction().aggregate(
                                         record_batch, time_column_name));

  ARROW_ASSIGN_OR_RAISE(tmax_scalar, arrow_utils::castTimestampScalar(
                                         tmax_scalar, arrow::TimeUnit::NANO));

  return std::static_pointer_cast<arrow::Int64Scalar>(tmax_scalar)->value;
}

StreamRecordBatchRequestHandlerBase::StreamRecordBatchRequestHandlerBase(
    const std::shared_ptr<IUDFAgent>& agent, bool provides_batch)
    : RecordBatchRequestHandler(agent, provides_batch) {}

agent::Response StreamRecordBatchRequestHandlerBase::snapshot() const {
  agent::Response response;

  response.mutable_snapshot()->set_snapshot(getPoints().SerializeAsString());

  return response;
}

agent::Response StreamRecordBatchRequestHandlerBase::restore(
    const agent::RestoreRequest& restore_request) {
  agent::Response response;
  if (restore_request.snapshot().empty()) {
    response.mutable_restore()->set_success(false);
    response.mutable_restore()->set_error(
        "Can't restore from empty snapshot");
    return response;
  }

  restorePointsFromSnapshotData(restore_request.snapshot());
  response.mutable_restore()->set_success(true);
  return response;
}

void StreamRecordBatchRequestHandlerBase::beginBatch(
    const agent::BeginBatch& batch) {
  agent::Response response;
  response.mutable_error()->set_error(
      "Invalid BeginBatch request, UDF wants stream data");
  getAgent()->writeResponse(response);
}

void StreamRecordBatchRequestHandlerBase::endBatch(
    const agent::EndBatch& batch) {
  agent::Response response;
  response.mutable_error()->set_error(
      "Invalid EndBatch request, UDF wants stream data");
  getAgent()->writeResponse(response);
}

TimerRecordBatchRequestHandlerBase::TimerRecordBatchRequestHandlerBase(
    const std::shared_ptr<IUDFAgent>& agent, bool provides_batch,
    uvw::Loop* loop)
    : StreamRecordBatchRequestHandlerBase(agent, provides_batch),
      emit_timer_(loop->resource<uvw::TimerHandle>()) {
  emit_timer_->on<uvw::TimerEvent>(
      [this](const uvw::TimerEvent& /* non-used */,
             const uvw::TimerHandle& /* non-used */) { handleBatch(); });
}

TimerRecordBatchRequestHandlerBase::TimerRecordBatchRequestHandlerBase(
    const std::shared_ptr<IUDFAgent>& agent, bool provides_batch,
    uvw::Loop* loop, const std::chrono::seconds& batch_interval)
    : StreamRecordBatchRequestHandlerBase(agent, provides_batch),
      emit_timer_(loop->resource<uvw::TimerHandle>()),
      emit_timeout_(batch_interval) {
  emit_timer_->on<uvw::TimerEvent>(
      [this](const uvw::TimerEvent& /* non-used */,
             const uvw::TimerHandle& /* non-used */) { handleBatch(); });
}

void TimerRecordBatchRequestHandlerBase::point(const agent::Point& point) {
  addPoint(point);
  if (!emit_timer_->active()) {
    emit_timer_->start(emit_timeout_, emit_timeout_);
  }
}

void TimerRecordBatchRequestHandlerBase::stop() {
  handleBatch();
  emit_timer_->stop();
  RequestHandler::stop();
}

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
