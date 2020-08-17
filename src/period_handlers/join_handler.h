#pragma once

#include <string>
#include <vector>

#include <arrow/api.h>

#include "period_handler.h"

class JoinHandler : public PeriodHandler {
 public:
  template <typename U>
  JoinHandler(U&& join_on_columns, std::string ts_column_name = "", int64_t tolerance = 0)
      : join_on_columns_(std::forward<U>(join_on_columns))
      , ts_column_name_(std::move(ts_column_name))
      , tolerance_(tolerance) {

  }

  arrow::Status handle(const std::deque<std::shared_ptr<arrow::Buffer>> &period,
                       std::shared_ptr<arrow::Buffer>& result) override;

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
    bool operator()(const JoinValue& v1, const JoinValue& v2) {
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
  arrow::Status getJoinKey(const std::shared_ptr<arrow::RecordBatch>& record_batch, size_t row_idx, JoinKey& join_key) const;

 private:
  std::vector<std::string> join_on_columns_;
  std::string ts_column_name_;
  int64_t tolerance_;
};


