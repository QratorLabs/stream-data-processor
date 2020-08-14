#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "node.h"
#include "data_handlers/data_handler.h"

class EvalNode : public Node {
 public:
  template <typename U>
  EvalNode(std::string name,
           U&& consumer,
           std::shared_ptr<DataHandler> data_handler)
      : Node(std::move(name), std::forward<U>(consumer))
      , data_handler_(std::move(data_handler))
      , buffer_builder_(std::make_shared<arrow::BufferBuilder>()) {
    configureNode();
  }

  void start() override;
  void handleData(const char *data, size_t length) override;
  void stop() override;

 private:
  void configureNode();

  void pass();
  arrow::Status processData(std::shared_ptr<arrow::Buffer>& processed_data);

 private:
  std::shared_ptr<DataHandler> data_handler_;
  std::shared_ptr<arrow::BufferBuilder> buffer_builder_;
};


