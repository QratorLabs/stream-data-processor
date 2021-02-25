#pragma once

#include <memory>
#include <string>

#include <arrow/api.h>

#include "kapacitor_udf/udf_agent.h"
#include "metadata/metadata.h"
#include "points_converter.h"
#include "record_batch_handlers/record_batch_handler.h"

#include "udf.pb.h"

namespace stream_data_processor {
namespace kapacitor_udf {
namespace storage_utils {

class IPointsStorage {
 public:
  virtual void addPoint(const agent::Point& point) = 0;
  virtual arrow::Status handleBatch() const = 0;
  virtual void clear() = 0;

  [[nodiscard]] virtual std::string snapshot() const = 0;
  virtual bool restore(const std::string& data) = 0;

  virtual void setPointsName(const std::string& name) = 0;

  [[nodiscard]] virtual bool providesBatch() const = 0;

  virtual ~IPointsStorage() = 0;

 protected:
  IPointsStorage() = default;

  IPointsStorage(const IPointsStorage& /* non-used */) = default;
  IPointsStorage& operator=(const IPointsStorage& /* non-used */) = default;

  IPointsStorage(IPointsStorage&& /* non-used */) = default;
  IPointsStorage& operator=(IPointsStorage&& /* non-used */) = default;
};

class PointsStorage : public IPointsStorage {
 public:
  PointsStorage(
      const IUDFAgent* agent,
      std::unique_ptr<convert_utils::PointsConverter>&& points_converter,
      std::unique_ptr<RecordBatchHandler>&& handler, bool provides_batch)
      : agent_(agent),
        points_converter_(std::move(points_converter)),
        handler_(std::move(handler)),
        provides_batch_(provides_batch) {}

  void addPoint(const agent::Point& point) override;
  arrow::Status handleBatch() const override;
  void clear() override;

  [[nodiscard]] std::string snapshot() const override;
  bool restore(const std::string& data) override;

  void setPointsName(const std::string& name) override;

  [[nodiscard]] bool providesBatch() const override {
    return provides_batch_;
  }

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
        (*batch_response->mutable_tags())[group_key] = group_value;
      } else {
        batch_response->set_byname(true);
      }
    }

    return arrow::Status::OK();
  }

  static arrow::Result<int64_t> getTMax(
      const arrow::RecordBatch& record_batch);

 private:
  const IUDFAgent* agent_;
  std::unique_ptr<convert_utils::PointsConverter> points_converter_;
  std::unique_ptr<RecordBatchHandler> handler_;
  bool provides_batch_;
  agent::PointBatch points_;
};

}  // namespace storage_utils
}  // namespace kapacitor_udf
}  // namespace stream_data_processor
