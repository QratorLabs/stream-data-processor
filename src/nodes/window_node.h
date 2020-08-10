#pragma once

#include <ctime>
#include <functional>
#include <string>
#include <deque>

#include <arrow/api.h>

#include "uvw.hpp"

#include "node.h"

class WindowNode : public Node {
 public:
  template <typename U>
  WindowNode(std::string name,
             U&& consumers,
             uint64_t window_range, uint64_t window_period,
             std::string ts_column_name)
      : Node(std::move(name), std::forward<U>(consumers))
      , window_period_(window_period)
      , ts_column_name_(std::move(ts_column_name))
      , separation_idx_(std::vector<size_t>(
          window_range / window_period + (window_range % window_period > 0 ? 1 : 0) - 1, 0
      )) {

  }

  void start() override;
  void handleData(const char *data, size_t length) override;
  void stop() override;

 private:
  arrow::Status appendData(const char *data, size_t length);
  void pass();

  void removeOldBuffers();
  arrow::Status buildWindow(std::shared_ptr<arrow::Buffer>& window_data);
  arrow::Status tsLowerBound(const std::shared_ptr<arrow::RecordBatch>& record_batch,
                             const std::function<bool (std::time_t)>& pred, size_t &lower_bound);

 private:
  uint64_t window_period_;
  std::string ts_column_name_;
  std::vector<size_t> separation_idx_;
  std::deque<std::shared_ptr<arrow::Buffer>> data_buffers_;
  std::time_t first_ts_in_current_batch_{0};
};


