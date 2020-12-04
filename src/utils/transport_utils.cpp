#include <cstdlib>
#include <sstream>
#include <string>

#include "transport_utils.h"

#include "string_utils.h"

namespace stream_data_processor {
namespace transport_utils {

const size_t TransportUtils::MESSAGE_SIZE_STRING_LENGTH{10};
const std::string TransportUtils::CONNECT_MESSAGE{"connect"};
const std::string TransportUtils::END_MESSAGE{"end"};

arrow::Result<std::shared_ptr<arrow::Buffer>> TransportUtils::wrapMessage(
    const std::shared_ptr<arrow::Buffer>&
        buffer) {  // TODO: Use ResizableBuffer
  arrow::BufferBuilder builder;
  auto message_size_string = getSizeString(buffer->size());
  ARROW_RETURN_NOT_OK(builder.Append(message_size_string.c_str(),
                                     message_size_string.size()));
  ARROW_RETURN_NOT_OK(builder.Append(buffer->data(), buffer->size()));

  std::shared_ptr<arrow::Buffer> terminated_buffer;
  ARROW_RETURN_NOT_OK(builder.Finish(&terminated_buffer));
  return terminated_buffer;
}

std::vector<std::pair<const char*, size_t>> TransportUtils::splitMessage(
    const char* message_data, size_t length) {
  std::vector<std::pair<const char*, size_t>> parts;
  size_t last_offset = 0;
  while (last_offset < length) {
    auto message_part_start_offset = last_offset + MESSAGE_SIZE_STRING_LENGTH;
    auto message_size_string =
        std::string(message_data + last_offset, MESSAGE_SIZE_STRING_LENGTH);
    auto message_size = parseMessageSize(message_size_string);
    parts.emplace_back(message_data + message_part_start_offset,
                       message_size);
    last_offset = message_part_start_offset + message_size;
  }

  return parts;
}

bool TransportUtils::send(zmq::socket_t& socket, const std::string& string,
                          zmq::send_flags flags) {
  zmq::message_t message(string.size());
  memcpy(message.data(), string.data(), string.size());

  auto rc = socket.send(message, flags);
  return rc.has_value();
}

std::string TransportUtils::receive(zmq::socket_t& socket,
                                    zmq::recv_flags flags) {
  zmq::message_t message;
  auto recv_result = socket.recv(message, flags);
  if (!recv_result.has_value()) {
    return std::string();
  }

  return std::string(static_cast<char*>(message.data()), message.size());
}

std::string TransportUtils::getSizeString(size_t size) {
  auto size_string = std::to_string(size);
  return std::string(MESSAGE_SIZE_STRING_LENGTH - size_string.size(), '0') +
         size_string;
}

size_t TransportUtils::parseMessageSize(const std::string& size_string) {
  auto start = size_string.find_first_not_of('0');
  if (start == std::string::npos) {
    return 0;
  }

  char* str_end = nullptr;
  auto message_size = std::strtol(size_string.substr(start).c_str(), &str_end,
                                  NUMBER_PARSING_BASE);
  if (errno == ERANGE) {
    return 0;
  }

  return message_size;
}

zmq::message_t TransportUtils::readMessage(zmq::socket_t& socket,
                                           zmq::recv_flags flags) {
  zmq::message_t message;
  auto recv_result = socket.recv(message, flags);
  if (!recv_result.has_value()) {
    throw std::runtime_error("Error while receiving message, error code: " +
                             std::to_string(zmq_errno()));
  }

  return message;
}

TransportUtils::Publisher::Publisher(
    std::shared_ptr<zmq::socket_t> publisher_socket,
    std::vector<std::shared_ptr<zmq::socket_t>> synchronize_sockets)
    : publisher_socket_(std::move(publisher_socket)),
      synchronize_sockets_(std::move(synchronize_sockets)),
      expected_subscribers_(synchronize_sockets_.size()) {}

bool TransportUtils::Publisher::isReady() const {
  return expected_subscribers_ <= 0;
}

std::shared_ptr<zmq::socket_t> TransportUtils::Publisher::publisher_socket() {
  return publisher_socket_;
}

std::vector<std::shared_ptr<zmq::socket_t>>&
TransportUtils::Publisher::synchronize_sockets() {
  return synchronize_sockets_;
}

void TransportUtils::Publisher::trySynchronize() {
  if (isReady()) {
    return;
  }

  send(*publisher_socket_, CONNECT_MESSAGE);
}

void TransportUtils::Publisher::addConnection() { --expected_subscribers_; }

TransportUtils::Subscriber::Subscriber(
    std::shared_ptr<zmq::socket_t> subscriber_socket,
    std::shared_ptr<zmq::socket_t> synchronize_socket)
    : subscriber_socket_(std::move(subscriber_socket)),
      synchronize_socket_(std::move(synchronize_socket)) {}

bool TransportUtils::Subscriber::isReady() const { return is_ready_; }

zmq::socket_t& TransportUtils::Subscriber::subscriber_socket() {
  return *subscriber_socket_;
}
zmq::socket_t& TransportUtils::Subscriber::synchronize_socket() {
  return *synchronize_socket_;
}

void TransportUtils::Subscriber::prepareForListening() { is_ready_ = true; }

}  // namespace transport_utils
}  // namespace stream_data_processor
