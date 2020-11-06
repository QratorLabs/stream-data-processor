#pragma once

#include <exception>
#include <string>

namespace stream_data_processor {
namespace kapacitor_udf {

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

}  // namespace kapacitor_udf
}  // namespace stream_data_processor
