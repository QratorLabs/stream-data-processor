#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "metadata.pb.h"
#include "udf.pb.h"

namespace stream_data_processor {
namespace kapacitor_udf {
namespace convert_utils {

class PointsConverter {
 public:
  virtual arrow::Result<arrow::RecordBatchVector> convertToRecordBatches(
      const agent::PointBatch& points) const = 0;

  virtual arrow::Result<agent::PointBatch> convertToPoints(
      const arrow::RecordBatchVector& record_batches) const = 0;

  virtual ~PointsConverter() = 0;

 protected:
  PointsConverter() = default;

  PointsConverter(const PointsConverter& /* non-used */) = default;
  PointsConverter& operator=(const PointsConverter& /* non-used */) = default;

  PointsConverter(PointsConverter&& /* non-used */) = default;
  PointsConverter& operator=(PointsConverter&& /* non-used */) = default;
};

class BasePointsConverter : public PointsConverter {
 public:
  struct PointsToRecordBatchesConversionOptions {
    std::string time_column_name;
    std::string measurement_column_name;
  };

 public:
  template <class OptionsType>
  explicit BasePointsConverter(OptionsType&& options)
      : options_(std::forward<OptionsType>(options)) {}

  arrow::Result<arrow::RecordBatchVector> convertToRecordBatches(
      const agent::PointBatch& points) const override;

  arrow::Result<agent::PointBatch> convertToPoints(
      const arrow::RecordBatchVector& record_batches) const override;

 private:
  template <typename T, typename BuilderType>
  static void addBuilders(
      const google::protobuf::Map<std::string, T>& data_map,
      std::map<std::string, BuilderType>* builders,
      arrow::MemoryPool* pool = arrow::default_memory_pool());

  template <typename T, typename BuilderType>
  static arrow::Status appendValues(
      const google::protobuf::Map<std::string, T>& data_map,
      std::map<std::string, BuilderType>* builders);

  template <typename BuilderType>
  static arrow::Status buildColumnArrays(
      arrow::ArrayVector* column_arrays, arrow::FieldVector* schema_fields,
      std::map<std::string, BuilderType>* builders,
      const std::shared_ptr<arrow::DataType>& data_type,
      metadata::ColumnType column_type);

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> convertPointsGroup(
      const agent::PointBatch& points, const std::string& group_string,
      const std::vector<size_t>& group_indexes) const;

 private:
  PointsToRecordBatchesConversionOptions options_;
};

class BasePointsConverterDecorator : public PointsConverter {
 public:
  template <class WrappeeType>
  explicit BasePointsConverterDecorator(WrappeeType&& wrappee)
      : wrappee_converter_(std::forward<WrappeeType>(wrappee)) {}

  arrow::Result<arrow::RecordBatchVector> convertToRecordBatches(
      const agent::PointBatch& points) const override {
    return wrappee_converter_->convertToRecordBatches(points);
  }

  arrow::Result<agent::PointBatch> convertToPoints(
      const arrow::RecordBatchVector& record_batches) const override {
    return wrappee_converter_->convertToPoints(record_batches);
  }

 private:
  std::shared_ptr<PointsConverter> wrappee_converter_;
};

}  // namespace convert_utils
}  // namespace kapacitor_udf
}  // namespace stream_data_processor
