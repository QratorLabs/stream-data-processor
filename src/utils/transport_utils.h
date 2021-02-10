#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include <uvw.hpp>
#include <zmq.hpp>
#include "consumers/consumer.h"
#include "nodes/node.h"
#include "producers/producer.h"

namespace stream_data_processor {
namespace transport_utils {

struct IPv4Endpoint {
  std::string host;
  uint16_t port;
};

class TransportUtils {
 public:
  static const size_t MESSAGE_SIZE_STRING_LENGTH;
  static const std::string CONNECT_MESSAGE;
  static const std::string END_MESSAGE;

 public:
  enum ZMQTransportType { INPROC, IPC, TCP };

  class Publisher {
   public:
    Publisher(
        std::shared_ptr<zmq::socket_t> publisher_socket,
        std::vector<std::shared_ptr<zmq::socket_t>> synchronize_sockets);

    [[nodiscard]] bool isReady() const;

    std::shared_ptr<zmq::socket_t> publisher_socket();
    std::vector<std::shared_ptr<zmq::socket_t>>& synchronize_sockets();

    void trySynchronize();
    void addConnection();

   private:
    std::shared_ptr<zmq::socket_t> publisher_socket_;
    std::vector<std::shared_ptr<zmq::socket_t>> synchronize_sockets_;
    int64_t expected_subscribers_;
  };

  class Subscriber {
   public:
    Subscriber(std::shared_ptr<zmq::socket_t> subscriber_socket,
               std::shared_ptr<zmq::socket_t> synchronize_socket);

    [[nodiscard]] bool isReady() const;

    zmq::socket_t& subscriber_socket();
    zmq::socket_t& synchronize_socket();

    void prepareForListening();

   private:
    std::shared_ptr<zmq::socket_t> subscriber_socket_;
    std::shared_ptr<zmq::socket_t> synchronize_socket_;
    bool is_ready_{false};
  };

 public:
  static arrow::Result<std::shared_ptr<arrow::Buffer>> wrapMessage(
      const std::shared_ptr<arrow::Buffer>&
          buffer);  // TODO: Use ResizableBuffer

  static std::vector<std::pair<const char*, size_t>> splitMessage(
      const char* message_data, size_t length);

  static bool send(zmq::socket_t& socket, const std::string& string,
                   zmq::send_flags flags = zmq::send_flags::none);
  static std::string receive(zmq::socket_t& socket,
                             zmq::recv_flags flags = zmq::recv_flags::none);
  static zmq::message_t readMessage(
      zmq::socket_t& socket, zmq::recv_flags flags = zmq::recv_flags::none);

 private:
  static constexpr int NUMBER_PARSING_BASE{10};

 private:
  static std::string getSizeString(size_t size);
  static size_t parseMessageSize(const std::string& size_string);
};

}  // namespace transport_utils
}  // namespace stream_data_processor
