#pragma once

#include <exception>
#include <fstream>
#include <string>

namespace stream_data_processor {
namespace uvarint_utils {

class EOFException : public std::exception {};

std::ostream& encode(std::ostream& writer, uint32_t value);
uint32_t decode(std::istream& reader);

}  // namespace uvarint_utils
}  // namespace stream_data_processor
