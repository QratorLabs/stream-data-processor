#pragma once

#include <memory>
#include <vector>

#include <arrow/api.h>

class ComputeUtils {
 public:
  static arrow::Status groupSortingByColumns(
      const std::vector<std::string>& column_names,
      const std::shared_ptr<arrow::RecordBatch>& record_batch,
      std::vector<std::shared_ptr<arrow::RecordBatch>>* grouped);

  static arrow::Status sortByColumn(
      const std::string& column_name,
      const std::shared_ptr<arrow::RecordBatch>& source,
      std::shared_ptr<arrow::RecordBatch>* target);

  static arrow::Status argMinMax(std::shared_ptr<arrow::Array> array,
                                 std::pair<size_t, size_t>* arg_min_max);

 private:
  static arrow::Status sort(
      const std::vector<std::string>& column_names, size_t i,
      const std::shared_ptr<arrow::RecordBatch>& source,
      std::vector<std::shared_ptr<arrow::RecordBatch>>* targets);
};
