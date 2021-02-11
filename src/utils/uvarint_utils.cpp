#include "uvarint_utils.h"

namespace stream_data_processor {
namespace uvarint_utils {

namespace {

constexpr uint32_t MAX_SHIFT_FOR_UINT32{32};
constexpr uint32_t PREVENT_MAX_SHIFT{28};
constexpr uint32_t UINT32_MASK = UINT32_MAX;
constexpr uint8_t SHIFT_SIZE = 7;
constexpr uint8_t VARINT_MORE_MASK = (1 << 7);
constexpr uint8_t VARINT_MASK = VARINT_MORE_MASK - 1;

}  // namespace

std::ostream& encode(std::ostream& writer, uint32_t value) {
  uint8_t bits = value & VARINT_MASK;
  value >>= SHIFT_SIZE;
  while (value > 0) {
    writer << static_cast<uint8_t>(VARINT_MORE_MASK | bits);
    bits = value & VARINT_MASK;
    value >>= SHIFT_SIZE;
  }

  return writer << bits;
}

uint32_t decode(std::istream& reader) {
  uint32_t result = 0;
  uint8_t shift = 0;
  while (true) {
    char byte = 0;
    reader.read(&byte, 1);
    if (reader.eof()) {
      throw EOFException();
    }

    if (shift >= PREVENT_MAX_SHIFT && ((byte & VARINT_MASK) >> 4) != 0) {
      throw std::runtime_error("decoding value is larger than 32bit uint");
    }

    result |= (static_cast<uint32_t>((byte & VARINT_MASK)) << shift);
    if ((byte & VARINT_MORE_MASK) == 0) {
      result &= UINT32_MASK;
      return result;
    }

    shift += SHIFT_SIZE;
    if (shift >= MAX_SHIFT_FOR_UINT32) {
      throw std::runtime_error(
          "too many bytes when decoding varint, larger than 32bit uint");
    }
  }
}

}  // namespace uvarint_utils
}  // namespace stream_data_processor
