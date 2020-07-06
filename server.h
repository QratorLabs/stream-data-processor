#pragma once

#include <chrono>
#include <iostream>
#include <memory>
#include <string>

#include <arrow/api.h>
#include <utility>
#include <uvw.hpp>

class Server {
 public:
  explicit Server(const std::shared_ptr<uvw::Loop>& loop);

  void start(const std::string& host, size_t port);

 private:
  std::shared_ptr<uvw::TCPHandle> tcp_;

  static const std::chrono::duration<uint64_t> SILENCE_TIMEOUT_;
};


class BufferBuilderWrapper {
 public:
  ~BufferBuilderWrapper() {
    std::cerr << "BufferBuilder deleted" << std::endl;
  }

 public:
  arrow::BufferBuilder buffer_builder_;
};


class Indicator {
 public:
  explicit Indicator(std::string id) : id_(std::move(id)) {

  }

  ~Indicator() {
    std::cerr << "Destructed " << id_ << std::endl;
  }

 private:
  std::string id_;
};
