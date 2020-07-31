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
  static const size_t MESSAGE_SIZE_STRING_LENGTH;

 public:
  static arrow::Status wrapMessage(const std::shared_ptr<arrow::Buffer>& buffer, // TODO: Use ResizableBuffer
                                 std::shared_ptr<arrow::Buffer>* terminated_buffer);

  static std::vector<std::pair<const char *, size_t>> splitMessage(const char *message_data, size_t length);

 private:
  static std::string getSizeString(size_t size);
  static size_t parseMessageSize(const std::string& size_string);
};


