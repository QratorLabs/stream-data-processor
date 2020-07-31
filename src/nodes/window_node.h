#pragma once

#include <ctime>
#include <functional>
#include <string>
#include <deque>

#include <arrow/api.h>

#include "uvw.hpp"

#include "pass_node_base.h"

class WindowNode : public PassNodeBase {
 public:
  WindowNode(std::string name,
             const std::shared_ptr<uvw::Loop>& loop,
             const IPv4Endpoint& listen_endpoint,
             const std::vector<IPv4Endpoint>& target_endpoints,
             uint64_t window_range, uint64_t window_period,
             std::string ts_column_name);

 private:
  void configureServer();

  arrow::Status appendData(const char *data, size_t length);
  void send();
  void stop();

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

  static const std::chrono::duration<uint64_t, std::milli> RETRY_DELAY;
};


