#pragma once

#include <memory>

#include <arrow/api.h>

class Consumer {
 public:
  virtual void start() = 0;
  virtual void consume(const std::shared_ptr<arrow::Buffer>& data) = 0;
  virtual void stop() = 0;
};
