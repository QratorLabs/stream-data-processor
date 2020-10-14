#pragma once

#include <exception>
#include <fstream>
#include <string>

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
};
