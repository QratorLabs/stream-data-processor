#pragma once

class Consumer {
 public:
  virtual void start() = 0;
  virtual void consume(const char* data, size_t length) = 0;
  virtual void stop() = 0;
};
