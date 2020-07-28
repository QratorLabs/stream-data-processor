#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

struct IPv4Endpoint {
  std::string host;
  uint16_t port;
};

class NetworkUtils {
 public:
  static const std::string TERMINATING_SYMBOLS;

 public:
  static arrow::Status terminate(const std::shared_ptr<arrow::Buffer>& buffer, // TODO: Use ResizableBuffer
                                 std::shared_ptr<arrow::Buffer>* terminated_buffer);

  static std::vector<std::pair<char*, size_t>> splitMessage(char* message_data, size_t length);
};


