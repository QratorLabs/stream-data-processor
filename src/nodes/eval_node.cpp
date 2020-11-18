#include <utility>

#include <spdlog/spdlog.h>

#include "eval_node.h"

namespace stream_data_processor {

void EvalNode::start() { spdlog::get(name_)->info("Node started"); }

void EvalNode::handleData(const char* data, size_t length) {
  spdlog::get(name_)->debug("Process data of size {}", length);
  arrow::Buffer data_buffer(reinterpret_cast<const uint8_t*>(data), length);
  std::vector<std::shared_ptr<arrow::Buffer>> processed_data;
  auto processing_status = processData(data_buffer, &processed_data);
  if (!processing_status.ok()) {
    spdlog::get(name_)->error(processing_status.ToString());
    return;
  }

  passData(processed_data);
}

void EvalNode::stop() {
  spdlog::get(name_)->info("Stopping node");
  for (auto& consumer : consumers_) { consumer->stop(); }
}

arrow::Status EvalNode::processData(
    const arrow::Buffer& data_buffer,
    std::vector<std::shared_ptr<arrow::Buffer>>* processed_data) {
  ARROW_RETURN_NOT_OK(data_handler_->handle(data_buffer, processed_data));
  return arrow::Status::OK();
}

}  // namespace stream_data_processor
