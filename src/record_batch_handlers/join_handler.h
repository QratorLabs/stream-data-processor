#pragma once

#include <string>
#include <vector>

#include <arrow/api.h>

#include "record_batch_handler.h"

class JoinHandler : public RecordBatchHandler {
 public:
  template <typename StringVectorType>
  explicit JoinHandler(StringVectorType&& join_on_columns,
                       std::string time_column_name, int64_t tolerance = 0)
      : join_on_columns_(std::forward<StringVectorType>(join_on_columns)),
        time_column_name_(std::move(time_column_name)),
        tolerance_(tolerance) {}

  arrow::Status handle(const arrow::RecordBatchVector& record_batches,
                       arrow::RecordBatchVector* result) override;

 private:
  struct JoinKey {
    std::string key_string;
    int64_t time;
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
  arrow::Status getJoinKey(
      const std::shared_ptr<arrow::RecordBatch>& record_batch, size_t row_idx,
      JoinKey* join_key) const;

 private:
  std::vector<std::string> join_on_columns_;
  std::string time_column_name_;
  int64_t tolerance_;
};
