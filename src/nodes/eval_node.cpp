#include <utility>

#include <spdlog/spdlog.h>

#include "eval_node.h"

void EvalNode::configureNode() {

}

void EvalNode::start() {
  spdlog::get(name_)->info("Node started");
}

void EvalNode::handleData(const char *data, size_t length) {
  spdlog::get(name_)->debug("Appending data of size {} to buffer", length);
  auto append_status = buffer_builder_->Append(data, length);
  if (!append_status.ok()) {
    spdlog::get(name_)->error(append_status.message());
  }

  pass();
}

void EvalNode::stop() {
  pass();
  spdlog::get(name_)->info("Stopping node");
  for (auto& consumer : consumers_) {
    consumer->stop();
  }
}

void EvalNode::pass() {
  std::shared_ptr<arrow::Buffer> processed_data;
  auto processing_status = processData(processed_data);
  if (!processing_status.ok()) {
    spdlog::get(name_)->error(processing_status.ToString());
    return;
  }

  passData(processed_data);
}

arrow::Status EvalNode::processData(std::shared_ptr<arrow::Buffer>& processed_data) {
  std::shared_ptr<arrow::Buffer> buffer;
  ARROW_RETURN_NOT_OK(buffer_builder_->Finish(&buffer));
  if (buffer->size() == 0) {
    return arrow::Status::CapacityError("No data to handle");
  }

  ARROW_RETURN_NOT_OK(data_handler_->handle(buffer, &processed_data));

  return arrow::Status::OK();
}
