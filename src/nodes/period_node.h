#pragma once

#include <ctime>
#include <functional>
#include <string>
#include <deque>

#include <arrow/api.h>

#include <uvw.hpp>

#include "node.h"
#include "period_handlers/period_handler.h"

class PeriodNode : public Node {
 public:
  PeriodNode(const std::string& name,
             uint64_t range, uint64_t period,
             std::string ts_column_name,
             std::shared_ptr<PeriodHandler> period_handler)
      : Node(name)
      , period_(period)
      , ts_column_name_(std::move(ts_column_name))
      , period_handler_(std::move(period_handler))
      /* Not quite fair window range. Actually it considers to be a multiple of window period */
      , separation_idx_(std::vector<size_t>(
          range / period + (range % period > 0 ? 1 : 0) - 1, 0
      )) {

  }

  template <typename ConsumerVectorType>
  PeriodNode(const std::string& name,
             ConsumerVectorType&& consumers,
             uint64_t range, uint64_t period,
             std::string ts_column_name,
             std::shared_ptr<PeriodHandler> period_handler)
      : Node(name, std::forward<ConsumerVectorType>(consumers))
      , period_(period)
      , ts_column_name_(std::move(ts_column_name))
      , period_handler_(std::move(period_handler))
      /* Not quite fair window range. Actually it considers to be a multiple of window period */
      , separation_idx_(std::vector<size_t>(
          range / period + (range % period > 0 ? 1 : 0) - 1, 0
      )) {

  }

  void start() override;
  void handleData(const char *data, size_t length) override;
  void stop() override;

 private:
  arrow::Status appendData(const char *data, size_t length);
  void pass();

  void removeOldBuffers();
  arrow::Status tsLowerBound(const std::shared_ptr<arrow::RecordBatch>& record_batch,
                             const std::function<bool (std::time_t)>& pred, size_t &lower_bound);

 private:
  uint64_t period_;
  std::string ts_column_name_;
  std::shared_ptr<PeriodHandler> period_handler_;
  std::vector<size_t> separation_idx_;
  std::deque<std::shared_ptr<arrow::Buffer>> data_buffers_;
  std::time_t first_ts_in_current_batch_{0};
};
