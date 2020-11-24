#include <utility>

#include <spdlog/spdlog.h>

#include "eval_node.h"

namespace stream_data_processor {

void EvalNode::start() { spdlog::get(name_)->info("Node started"); }

void EvalNode::handleData(const char* data, size_t length) {
  spdlog::get(name_)->debug("Process data of size {}", length);
  arrow::Buffer data_buffer(reinterpret_cast<const uint8_t*>(data), length);
  auto processed_data = data_handler_->handle(data_buffer);
  if (!processed_data.ok()) {
    spdlog::get(name_)->error(processed_data.status().message());
    return;
  }

  passData(processed_data.ValueOrDie());
}

void EvalNode::stop() {
  spdlog::get(name_)->info("Stopping node");
  for (auto& consumer : consumers_) { consumer->stop(); }
}

}  // namespace stream_data_processor
