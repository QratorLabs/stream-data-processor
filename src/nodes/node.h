#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>

#include "consumers/consumer.h"

class Node {
 public:
  explicit Node(std::string name)
      : name_(std::move(name))
      , logger_(spdlog::basic_logger_mt(name_, "logs/" + name_ + ".txt", true)) {
    spdlog::get(name_)->info("Node created");
  }

  template <typename ConsumerVectorType>
  Node(std::string name, ConsumerVectorType&& consumers)
      : name_(std::move(name))
      , logger_(spdlog::basic_logger_mt(name_, "logs/" + name_ + ".txt", true))
      , consumers_(std::forward<ConsumerVectorType>(consumers)) {
    spdlog::get(name_)->info("Node created");
  }

  void log(const std::string& message, spdlog::level::level_enum level = spdlog::level::info);

  virtual void start() = 0;
  virtual void handleData(const char* data, size_t length) = 0;
  virtual void stop() = 0;

  [[nodiscard]] const std::string& getName() const;

  void addConsumer(const std::shared_ptr<Consumer>& consumer);

 protected:
  void passData(const std::shared_ptr<arrow::Buffer>& data);

 protected:
  std::string name_;
  std::shared_ptr<spdlog::logger> logger_;
  std::vector<std::shared_ptr<Consumer>> consumers_;
};
