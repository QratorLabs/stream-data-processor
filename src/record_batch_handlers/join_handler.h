#pragma once

#include <string>
#include <vector>

#include <arrow/api.h>

#include "record_batch_handler.h"

namespace stream_data_processor {

class JoinHandler : public RecordBatchHandler {
 public:
  template <typename StringVectorType>
  explicit JoinHandler(StringVectorType&& join_on_columns,
                       int64_t tolerance = 0)
      : join_on_columns_(std::forward<StringVectorType>(join_on_columns)),
        tolerance_(tolerance) {}

  arrow::Result<arrow::RecordBatchVector> handle(
      const std::shared_ptr<arrow::RecordBatch>& record_batch) override;

  arrow::Result<arrow::RecordBatchVector> handle(
      const arrow::RecordBatchVector& record_batches) override;

 private:
  struct JoinKey {
    std::string key_string;
    int64_t time{0};
  };

  struct JoinValue {
    size_t record_batch_idx;
    size_t row_idx;
    int64_t time;
  };

  class JoinValueCompare {
   public:
    bool operator()(const JoinValue& v1, const JoinValue& v2) const {
      if (v1.time == v2.time) {
        if (v1.record_batch_idx == v2.record_batch_idx) {
          return v1.row_idx < v2.row_idx;
        } else {
          return v1.record_batch_idx < v2.record_batch_idx;
        }
      } else {
        return v1.time < v2.time;
      }
    }
  };

 private:
  arrow::Result<JoinKey> getJoinKey(
      const std::shared_ptr<arrow::RecordBatch>& record_batch, size_t row_idx,
      std::string time_column_name) const;

 private:
  std::vector<std::string> join_on_columns_;
  int64_t tolerance_;
};

}  // namespace stream_data_processor
