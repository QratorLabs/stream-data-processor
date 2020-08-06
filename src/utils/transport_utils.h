#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>

#include <zmq.hpp>

struct IPv4Endpoint {
  std::string host;
  uint16_t port;
};

class TransportUtils {
 public:
  static const size_t MESSAGE_SIZE_STRING_LENGTH;
  static const std::string CONNECT_MESSAGE;

  class Publisher {
   public:
    Publisher(zmq::socket_t&& publisher_socket, zmq::socket_t&& synchronize_socket, int64_t expected_subscribers = 0);

    Publisher(const Publisher&) = delete;
    Publisher& operator=(const Publisher&) = delete;

    Publisher(Publisher&& other) noexcept;
    Publisher& operator=(Publisher&& other) = delete;

    bool isReady() const;

    zmq::socket_t& publisher_socket();
    zmq::socket_t& synchronize_socket();

    void startConnecting();

   private:
    zmq::socket_t publisher_socket_;
    zmq::socket_t synchronize_socket_;
    std::atomic<int64_t> expected_subscribers_;
  };

  class Subscriber {
   public:
    Subscriber(zmq::socket_t&& subscriber_socket, zmq::socket_t&& synchronize_socket);

    Subscriber(const Subscriber&) = delete;
    Subscriber& operator=(const Subscriber&) = delete;

    Subscriber(Subscriber&& other) noexcept;
    Subscriber& operator=(Subscriber&& other) = delete;

    [[nodiscard]] bool isReady() const;

    zmq::socket_t& subscriber_socket();
    zmq::socket_t& synchronize_socket();

    void confirmConnection();

   private:
    zmq::socket_t subscriber_socket_;
    zmq::socket_t synchronize_socket_;
    bool is_ready_{false};
  };

 public:
  static arrow::Status wrapMessage(const std::shared_ptr<arrow::Buffer>& buffer, // TODO: Use ResizableBuffer
                                 std::shared_ptr<arrow::Buffer>* terminated_buffer);

  static std::vector<std::pair<const char *, size_t>> splitMessage(const char *message_data, size_t length);

  inline static bool send(zmq::socket_t & socket, const std::string & string, int flags = 0);
  inline static std::string receive(zmq::socket_t & socket, int flags = 0);

 private:
  static std::string getSizeString(size_t size);
  static size_t parseMessageSize(const std::string& size_string);
};


