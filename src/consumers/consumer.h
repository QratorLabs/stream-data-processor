#pragma once

#include <memory>

#include <arrow/api.h>

namespace stream_data_processor {

class Consumer {
 public:
  virtual void start() = 0;
  virtual void consume(std::shared_ptr<arrow::Buffer> data) = 0;
  virtual void stop() = 0;
};

}  // namespace stream_data_processor
