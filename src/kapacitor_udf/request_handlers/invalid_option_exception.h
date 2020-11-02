#pragma once

#include <exception>
#include <string>

class InvalidOptionException : public std::exception {
 public:
  explicit InvalidOptionException(const std::string& message)
      : message_(message) {}

  [[nodiscard]] const char* what() const noexcept override {
    return message_.c_str();
  }

 private:
  std::string message_;
};