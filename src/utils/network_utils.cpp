#include "network_utils.h"

const std::string NetworkUtils::TERMINATING_SYMBOLS{"\r\n"};

arrow::Status NetworkUtils::terminate(const std::shared_ptr<arrow::Buffer>& buffer, // TODO: Use ResizableBuffer
                        std::shared_ptr<arrow::Buffer>* terminated_buffer) {
  arrow::BufferBuilder builder;
  ARROW_RETURN_NOT_OK(builder.Append(buffer->data(), buffer->size()));
  ARROW_RETURN_NOT_OK(builder.Append(TERMINATING_SYMBOLS.data(), TERMINATING_SYMBOLS.size()));
  ARROW_RETURN_NOT_OK(builder.Finish(terminated_buffer));
  return arrow::Status::OK();
}

std::vector<std::pair<char*, size_t>> NetworkUtils::splitMessage(char* message_data, size_t length) {
  std::vector<std::pair<char*, size_t>> parts;
  size_t last_offset = 0;
  for (size_t i = 0; i < length; ++i) {
    bool found = true;
    for (size_t j = 0; j < TERMINATING_SYMBOLS.size(); ++j) {
      if (i + j >= length || message_data[i + j] != TERMINATING_SYMBOLS[j]) {
        found = false;
      }
    }

    if (found) {
      parts.emplace_back(message_data + last_offset, i - last_offset);
      last_offset = i + TERMINATING_SYMBOLS.size();
    }
  }

  if (last_offset == 0) {
    parts.emplace_back(message_data, length);
  }

  return parts;
}
