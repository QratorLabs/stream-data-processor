#include <utility>

#include <spdlog/spdlog.h>

#include "eval_node.h"

namespace stream_data_processor {

void EvalNode::start() { log("Node started"); }

void EvalNode::handleData(const char* data, size_t length) {
  log(fmt::format("Process data of size {}", length), spdlog::level::debug);
  arrow::Buffer data_buffer(reinterpret_cast<const uint8_t*>(data), length);
  auto processed_data = data_handler_->handle(data_buffer);
  if (!processed_data.ok()) {
    log(processed_data.status().message(), spdlog::level::err);
    return;
  }

  passData(processed_data.ValueOrDie());
}

void EvalNode::stop() {
  log("Stopping node");
  stopConsumers();
}

}  // namespace stream_data_processor
