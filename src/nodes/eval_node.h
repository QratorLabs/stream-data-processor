#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "data_handlers/data_handler.h"
#include "node.h"

namespace stream_data_processor {

class EvalNode : public Node {
 public:
  EvalNode(const std::string& name, std::shared_ptr<DataHandler> data_handler)
      : Node(name), data_handler_(std::move(data_handler)) {}

  template <typename ConsumerVectorType>
  EvalNode(const std::string& name, ConsumerVectorType&& consumers,
           std::shared_ptr<DataHandler> data_handler)
      : Node(name, std::forward<ConsumerVectorType>(consumers)),
        data_handler_(std::move(data_handler)) {}

  void start() override;
  void handleData(const char* data, size_t length) override;
  void stop() override;

 private:
  std::shared_ptr<DataHandler> data_handler_;
};

}  // namespace stream_data_processor
