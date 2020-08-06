#include <cstdlib>
#include <sstream>
#include <string>

#include "transport_utils.h"

#include "string_utils.h"

const size_t TransportUtils::MESSAGE_SIZE_STRING_LENGTH{10};
const std::string TransportUtils::CONNECT_MESSAGE{"connect"};

arrow::Status TransportUtils::wrapMessage(const std::shared_ptr<arrow::Buffer>& buffer, // TODO: Use ResizableBuffer
                        std::shared_ptr<arrow::Buffer>* terminated_buffer) {
  arrow::BufferBuilder builder;
  auto message_size_string = getSizeString(buffer->size());
  ARROW_RETURN_NOT_OK(builder.Append(message_size_string.c_str(), message_size_string.size()));
  ARROW_RETURN_NOT_OK(builder.Append(buffer->data(), buffer->size()));
  ARROW_RETURN_NOT_OK(builder.Finish(terminated_buffer));
  return arrow::Status::OK();
}

std::vector<std::pair<const char *, size_t>> TransportUtils::splitMessage(const char *message_data, size_t length) {
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

inline bool TransportUtils::send(zmq::socket_t & socket, const std::string & string, int flags) {
  zmq::message_t message(string.size());
  memcpy (message.data(), string.data(), string.size());

  bool rc = socket.send(message, flags);
  return (rc);
}

inline std::string TransportUtils::receive(zmq::socket_t & socket, int flags) {
  zmq::message_t message;
  socket.recv(&message, flags);

  return std::string(static_cast<char*>(message.data()), message.size());
}

std::string TransportUtils::getSizeString(size_t size) {
  auto size_string = std::to_string(size);
  return std::string(MESSAGE_SIZE_STRING_LENGTH - size_string.size(), '0') + size_string;
}

size_t TransportUtils::parseMessageSize(const std::string &size_string) {
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

TransportUtils::Publisher::Publisher(zmq::socket_t &&publisher_socket,
                                     zmq::socket_t &&synchronize_socket,
                                     int64_t expected_subscribers)
                                     : publisher_socket_(std::move(publisher_socket))
                                     , synchronize_socket_(std::move(synchronize_socket))
                                     , expected_subscribers_(expected_subscribers) {

}

TransportUtils::Publisher::Publisher(Publisher&& other) noexcept
    : publisher_socket_(std::move(other.publisher_socket_))
    , synchronize_socket_(std::move(other.synchronize_socket_))
    , expected_subscribers_(other.expected_subscribers_.load()) {

}

bool TransportUtils::Publisher::isReady() const {
  return expected_subscribers_ <= 0;
}

zmq::socket_t &TransportUtils::Publisher::publisher_socket() {
  return publisher_socket_;
}

zmq::socket_t &TransportUtils::Publisher::synchronize_socket() {
  return synchronize_socket_;
}

void TransportUtils::Publisher::startConnecting() {
  while (expected_subscribers_ > 0) {
    send(publisher_socket_, CONNECT_MESSAGE);
    zmq::message_t message;
    auto recv_result = synchronize_socket_.recv(message, zmq::recv_flags::dontwait);
    if (recv_result.has_value()) {
      --expected_subscribers_;
    }
  }
}

TransportUtils::Subscriber::Subscriber(zmq::socket_t &&subscriber_socket, zmq::socket_t &&synchronize_socket)
    : subscriber_socket_(std::move(subscriber_socket)), synchronize_socket_(std::move(synchronize_socket)) {

}

TransportUtils::Subscriber::Subscriber(TransportUtils::Subscriber &&other) noexcept
    : subscriber_socket_(std::move(other.subscriber_socket_))
    , synchronize_socket_(std::move(other.synchronize_socket_)) {

}

bool TransportUtils::Subscriber::isReady() const {
  return is_ready_;
}

zmq::socket_t &TransportUtils::Subscriber::subscriber_socket() {
  return subscriber_socket_;
}
zmq::socket_t &TransportUtils::Subscriber::synchronize_socket() {
  return synchronize_socket_;
}

void TransportUtils::Subscriber::confirmConnection() {
  if (send(synchronize_socket_, "")) {
    is_ready_ = true;
  }
}
