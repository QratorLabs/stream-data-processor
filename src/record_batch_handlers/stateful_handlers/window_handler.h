#pragma once

#include <chrono>
#include <deque>

#include "record_batch_handlers/record_batch_handler.h"

namespace stream_data_processor {

class WindowHandler : public RecordBatchHandler {
 public:
  struct Options {
    std::chrono::seconds period;
    std::chrono::seconds every;
    bool fill_period;
  };

  template <typename OptionsType>
  explicit WindowHandler(OptionsType&& options)
      : options_(std::forward<OptionsType>(options)) {}

  arrow::Result<arrow::RecordBatchVector> handle(
      const std::shared_ptr<arrow::RecordBatch>& record_batch) override;

 private:
  arrow::Result<size_t> tsLowerBound(
      const arrow::RecordBatch& record_batch,
      const std::function<bool(std::time_t)>& pred,
      const std::string& time_column_name);

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> emitWindow();
  arrow::Status removeOldRecords();

 private:
  Options options_;
  std::deque<std::shared_ptr<arrow::RecordBatch>> buffered_record_batches_;
  std::time_t next_emit_{0};
};

}  // namespace stream_data_processor
