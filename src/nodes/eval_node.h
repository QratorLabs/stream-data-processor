#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <iostream>

#include "uvw.hpp"

#include "pass_node_base.h"
#include "data_handlers/data_handler.h"

class EvalNode : public PassNodeBase {
 public:
  EvalNode(std::string name,
           const std::shared_ptr<uvw::Loop>& loop,
           TransportUtils::Subscriber&& subscriber,
           TransportUtils::Publisher&& publisher,
           std::shared_ptr<DataHandler> data_handler);

 private:
  void configureServer();

  arrow::Status appendData(const char *data, size_t length);
  arrow::Status processData(std::shared_ptr<arrow::Buffer>& processed_data);
  void send();
  void stop();

 private:
  std::shared_ptr<DataHandler> data_handler_;
  std::shared_ptr<uvw::TimerHandle> timer_;
  std::shared_ptr<arrow::BufferBuilder> buffer_builder_;

  static const std::chrono::duration<uint64_t> SILENCE_TIMEOUT;
};


