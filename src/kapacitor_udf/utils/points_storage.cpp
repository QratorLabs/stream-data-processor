#include <spdlog/spdlog.h>

#include "grouping_utils.h"
#include "points_storage.h"
#include "record_batch_handlers/aggregate_functions/aggregate_functions.h"
#include "utils/utils.h"

namespace stream_data_processor {
namespace kapacitor_udf {
namespace storage_utils {

IPointsStorage::~IPointsStorage() = default;

void PointsStorage::addPoint(const agent::Point& point) {
  auto new_point = points_.mutable_points()->Add();
  new_point->CopyFrom(point);
}

arrow::Status PointsStorage::handleBatch() const {
  agent::Response response;

  if (points_converter_ == nullptr) {
    response.mutable_error()->set_error("PointsConverter is not provided");
    agent_->writeResponse(response);
    return arrow::Status::Invalid(response.error().error());
  }

  if (handler_ == nullptr) {
    response.mutable_error()->set_error("RecordBatchHandler is not provided");
    agent_->writeResponse(response);
    return arrow::Status::Invalid(response.error().error());
  }

  if (points_.points_size() == 0) {
    return arrow::Status::OK();
  }

  ARROW_ASSIGN_OR_RAISE(auto record_batches,
                        points_converter_->convertToRecordBatches(points_));

  ARROW_ASSIGN_OR_RAISE(auto result_batches,
                        handler_->handle(record_batches));

  for (auto& result_batch : result_batches) {
    ARROW_ASSIGN_OR_RAISE(auto response_points,
                          points_converter_->convertToPoints({result_batch}));

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
      agent_->writeResponse(response);
    }

    for (auto& point : response_points.points()) {
      response.mutable_point()->CopyFrom(point);
      agent_->writeResponse(response);
    }

    if (provides_batch_) {
      response.mutable_end()->CopyFrom(end_result.ValueOrDie());
      agent_->writeResponse(response);
    }
  }

  return arrow::Status::OK();
}

void PointsStorage::clear() { points_.mutable_points()->Clear(); }

std::string PointsStorage::snapshot() const {
  return points_.SerializeAsString();
}

bool PointsStorage::restore(const std::string& data) {
  points_.mutable_points()->Clear();
  return points_.ParseFromString(data);
}

void PointsStorage::setPointsName(const std::string& name) {
  for (auto& point : *points_.mutable_points()) { point.set_name(name); }
}

arrow::Result<agent::BeginBatch> PointsStorage::getBeginBatchResponse(
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

arrow::Result<agent::EndBatch> PointsStorage::getEndBatchResponse(
    const arrow::RecordBatch& record_batch) {
  agent::EndBatch end_batch_response;
  ARROW_ASSIGN_OR_RAISE(*end_batch_response.mutable_name(),
                        metadata::getMeasurementAndValidate(record_batch));

  ARROW_ASSIGN_OR_RAISE(*end_batch_response.mutable_group(),
                        getGroupString(record_batch));

  ARROW_RETURN_NOT_OK(
      setGroupTagsAndByName(&end_batch_response, record_batch));

  ARROW_ASSIGN_OR_RAISE(auto tmax_value, getTMax(record_batch));
  end_batch_response.set_tmax(tmax_value);

  return end_batch_response;
}

arrow::Result<std::string> PointsStorage::getGroupString(
    const arrow::RecordBatch& record_batch) {
  std::string measurement_column_name;
  ARROW_ASSIGN_OR_RAISE(
      measurement_column_name,
      metadata::getMeasurementColumnNameMetadata(record_batch));

  ARROW_ASSIGN_OR_RAISE(auto column_types,
                        metadata::getColumnTypes(record_batch));

  auto group = metadata::extractGroup(record_batch);
  return grouping_utils::encode(group, measurement_column_name, column_types);
}

arrow::Result<int64_t> PointsStorage::getTMax(
    const arrow::RecordBatch& record_batch) {
  ARROW_ASSIGN_OR_RAISE(auto time_column_name,
                        metadata::getTimeColumnNameMetadata(record_batch));

  ARROW_ASSIGN_OR_RAISE(
      auto tmax_scalar,
      LastAggregateFunction().aggregate(record_batch, time_column_name));

  ARROW_ASSIGN_OR_RAISE(tmax_scalar, arrow_utils::castTimestampScalar(
                                         tmax_scalar, arrow::TimeUnit::NANO));

  return std::static_pointer_cast<arrow::TimestampScalar>(tmax_scalar)->value;
}

}  // namespace storage_utils
}  // namespace kapacitor_udf
}  // namespace stream_data_processor
