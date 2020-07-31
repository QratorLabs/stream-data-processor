#include <cstdlib>
#include <sstream>
#include <string>

#include "network_utils.h"

#include "string_utils.h"

const size_t NetworkUtils::MESSAGE_SIZE_STRING_LENGTH{10};

arrow::Status NetworkUtils::wrapMessage(const std::shared_ptr<arrow::Buffer>& buffer, // TODO: Use ResizableBuffer
                        std::shared_ptr<arrow::Buffer>* terminated_buffer) {
  arrow::BufferBuilder builder;
  auto message_size_string = getSizeString(buffer->size());
  ARROW_RETURN_NOT_OK(builder.Append(message_size_string.c_str(), message_size_string.size()));
  ARROW_RETURN_NOT_OK(builder.Append(buffer->data(), buffer->size()));
  ARROW_RETURN_NOT_OK(builder.Finish(terminated_buffer));
  return arrow::Status::OK();
}

std::vector<std::pair<const char *, size_t>> NetworkUtils::splitMessage(const char *message_data, size_t length) {
  std::vector<std::pair<const char*, size_t>> parts;
  size_t last_offset = 0;
  while (last_offset < length) {
    auto message_part_start_offset = last_offset + MESSAGE_SIZE_STRING_LENGTH;
    auto message_size_string = std::string(message_data + last_offset, MESSAGE_SIZE_STRING_LENGTH);
    auto message_size = parseMessageSize(message_size_string);
    parts.emplace_back(message_data + message_part_start_offset, message_size);
    last_offset = message_part_start_offset + message_size;
  }

  return parts;
}

std::string NetworkUtils::getSizeString(size_t size) {
  auto size_string = std::to_string(size);
  return std::string(MESSAGE_SIZE_STRING_LENGTH - size_string.size(), '0') + size_string;
}

size_t NetworkUtils::parseMessageSize(const std::string &size_string) {
  auto start = size_string.find_first_not_of('0');
  if (start == std::string::npos) {
    return 0;
  }

  char* str_end;
  auto message_size = std::strtol(size_string.substr(start).c_str(), &str_end, 10);
  if (errno == ERANGE) {
    return 0;
  }

  return message_size;
}
