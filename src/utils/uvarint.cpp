#include "uvarint.h"

const uint32_t UVarIntCoder::UINT32_MASK = UINT32_MAX;
const uint8_t UVarIntCoder::SHIFT_SIZE = 7;
const uint8_t UVarIntCoder::VARINT_MORE_MASK = (1 << 7);
const uint8_t UVarIntCoder::VARINT_MASK = VARINT_MORE_MASK - 1;

std::ostream& UVarIntCoder::encode(std::ostream& writer, uint32_t value) {
  uint8_t bits = value & VARINT_MASK;
  value >>= SHIFT_SIZE;
  while (value > 0) {
    writer << static_cast<uint8_t>(VARINT_MORE_MASK | bits);
    bits = value & VARINT_MASK;
    value >>= SHIFT_SIZE;
  }

  return writer << bits;
}

uint32_t UVarIntCoder::decode(std::istream& reader) {
  uint32_t result = 0;
  uint8_t shift = 0;
  while (true) {
    char byte = 0;
    reader.read(&byte, 1);
    if (reader.eof()) {
      throw EOFException();
    }

    if (shift >= 28 && ((byte & VARINT_MASK) >> 4) != 0) {
      throw std::runtime_error("decoding value is larger than 32bit uint");
    }

    result |= (static_cast<uint32_t>((byte & VARINT_MASK)) << shift);
    if ((byte & VARINT_MORE_MASK) == 0) {
      result &= UINT32_MASK;
      return result;
    }

    shift += SHIFT_SIZE;
    if (shift >= 32) {
      throw std::runtime_error(
          "too many bytes when decoding varint, larger than 32bit uint");
    }
  }
}
