#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>
#include <unordered_set>

#include <arrow/api.h>

#include "udf.pb.h"

class DataConverter {
 public:
  struct PointsToRecordBatchesConversionOptions {
    std::string time_column_name;
    std::string measurement_column_name;
  };

  struct RecordBatchesToPointsConversionOptions {
    std::string time_column_name;
    std::string measurement_column_name;
    std::unordered_set<std::string> tag_columns_names;
  };

 public:
  static arrow::Status convertTableToRecordBatch(const std::shared_ptr<arrow::Table>& table,
                                                 std::shared_ptr<arrow::RecordBatch>* record_batch);

  static arrow::Status concatenateRecordBatches(const std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batches,
                                                std::shared_ptr<arrow::RecordBatch>* target);

  static arrow::Status convertToRecordBatches(const agent::PointBatch& points,
                                              arrow::RecordBatchVector* record_batches,
                                              const PointsToRecordBatchesConversionOptions& options);
  static arrow::Status convertToPoints(const arrow::RecordBatchVector& record_batches,
                                       agent::PointBatch* points,
                                       const RecordBatchesToPointsConversionOptions& options);

 private:
  template <typename T, typename BuilderType>
  static void addBuilders(const google::protobuf::Map<std::string, T>& data_map,
                          std::map<std::string, BuilderType>* builders,
                          arrow::MemoryPool* pool = arrow::default_memory_pool());

  template <typename T, typename BuilderType>
  static arrow::Status appendValues(const google::protobuf::Map<std::string, T>& data_map,
                                    std::map<std::string, BuilderType>* builders);

  template <typename BuilderType>
  static arrow::Status buildColumnArrays(arrow::ArrayVector* column_arrays,
                                         arrow::FieldVector* schema_fields,
                                         std::map<std::string, BuilderType>* builders,
                                         const std::shared_ptr<arrow::DataType>& data_type);
};
