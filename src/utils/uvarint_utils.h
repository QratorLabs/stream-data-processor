#pragma once

#include <exception>
#include <fstream>
#include <string>

namespace stream_data_processor {
namespace uvarint_utils {

class EOFException : public std::exception {};

class UVarIntCoder {
 public:
  static std::ostream& encode(std::ostream& writer, uint32_t value);
  static uint32_t decode(std::istream& reader);

 private:
  static const uint32_t UINT32_MASK;
  static const uint8_t SHIFT_SIZE;
  static const uint8_t VARINT_MORE_MASK;
  static const uint8_t VARINT_MASK;
  static const uint32_t MAX_SHIFT_FOR_UINT32{32};
  static const uint32_t PREVENT_MAX_SHIFT{28};
};

}  // namespace uvarint_utils
}  // namespace stream_data_processor
