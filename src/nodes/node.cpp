#include "node.h"

#include <spdlog/spdlog.h>

void Node::passData(const std::shared_ptr<arrow::Buffer> &data) {
  spdlog::get(name_)->info("Passing data of size {}", data->size());
  for (auto& consumer : consumers_) {
    try {
      consumer->consume(reinterpret_cast<const char *>(data->data()), data->size());
    } catch (const std::exception &e) {
      spdlog::get(name_)->error(e.what());
    }
  }
}

void Node::log(const std::string &message, spdlog::level::level_enum level) {
  spdlog::get(name_)->log(level, message);
}
