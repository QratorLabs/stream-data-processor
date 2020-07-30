#include "network_utils.h"

#include "string_utils.h"

const std::string NetworkUtils::TERMINATING_SYMBOLS{"\r\n"};

arrow::Status NetworkUtils::terminate(const std::shared_ptr<arrow::Buffer>& buffer, // TODO: Use ResizableBuffer
                        std::shared_ptr<arrow::Buffer>* terminated_buffer) {
  arrow::BufferBuilder builder;
  ARROW_RETURN_NOT_OK(builder.Append(buffer->data(), buffer->size()));
  ARROW_RETURN_NOT_OK(builder.Append(TERMINATING_SYMBOLS.data(), TERMINATING_SYMBOLS.size()));
  ARROW_RETURN_NOT_OK(builder.Finish(terminated_buffer));
  return arrow::Status::OK();
}

std::vector<std::string> NetworkUtils::splitMessage(char* message_data, size_t length) {
  auto message_parts = StringUtils::split(std::string(message_data, length), TERMINATING_SYMBOLS);
  while (!message_parts.empty() && message_parts.end()->empty()) {
    message_parts.pop_back();
  }

  return message_parts;
}
