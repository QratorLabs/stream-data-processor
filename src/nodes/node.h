#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>

#include "consumers/consumer.h"

namespace stream_data_processor {

class Node {
 public:
  explicit Node(std::string name)
      : name_(std::move(name)),
        logger_(
            spdlog::basic_logger_mt(name_, "logs/" + name_ + ".txt", true)) {
    logger_->info("Node created");
  }

  template <typename ConsumerVectorType>
  Node(std::string name, ConsumerVectorType&& consumers)
      : name_(std::move(name)),
        logger_(
            spdlog::basic_logger_mt(name_, "logs/" + name_ + ".txt", true)),
        consumers_(std::forward<ConsumerVectorType>(consumers)) {
    logger_->info("Node created");
  }

  virtual ~Node() = default;

  void log(const std::string& message,
           spdlog::level::level_enum level = spdlog::level::info);

  virtual void start() = 0;
  virtual void handleData(const char* data, size_t length) = 0;
  virtual void stop() = 0;

  [[nodiscard]] const std::string& getName() const;

  void addConsumer(std::shared_ptr<Consumer> consumer);

 protected:
  void passData(const std::vector<std::shared_ptr<arrow::Buffer>>& data);
  void stopConsumers();

  Node() = default;

  Node(const Node& /* non-used */) = delete;
  Node& operator=(const Node& /* non-used */) = delete;

  Node(Node&& /* non-used */) = default;
  Node& operator=(Node&& /* non-used */) = default;

 private:
  std::string name_;
  std::shared_ptr<spdlog::logger> logger_;
  std::vector<std::shared_ptr<Consumer>> consumers_;
};

}  // namespace stream_data_processor
