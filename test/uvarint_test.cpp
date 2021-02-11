#include <sstream>
#include <string>
#include <vector>

#include <catch2/catch.hpp>

#include "utils/uvarint_utils.h"

using namespace stream_data_processor;

using uvarint_utils::EOFException;

TEST_CASE("encode unsigned varint 300", "[uvarint_utils]") {
  std::ostringstream ss;
  uvarint_utils::encode(ss, 300);
  REQUIRE(ss.str() == "\xAC\x02");
}

TEST_CASE("decode unsigned varint 300", "[uvarint_utils]") {
  std::istringstream ss("\xAC\x02");
  auto value = uvarint_utils::decode(ss);
  REQUIRE(value == 300);
}

TEST_CASE("encode unsigned varint 12", "[uvarint_utils]") {
  std::ostringstream ss;
  uvarint_utils::encode(ss, 12);
  REQUIRE(ss.str() == "\x0C");
}

TEST_CASE("decode unsigned varint 12 as part of data", "[uvarint_utils]") {
  std::istringstream
      ss("\x0c\x1a\x0a\x08\xb0\xbd\xbc\xf5\x82\xc3\xd1\x9c\x16");
  auto value = uvarint_utils::decode(ss);
  REQUIRE(value == 12);
}

TEST_CASE(
    "throws EOFException when trying to decode sequence with all-first-1",
    "[uvarint_utils]") {
  std::istringstream ss("\xAC\xAC");
  CHECK_THROWS_AS(uvarint_utils::decode(ss), EOFException);
}

TEST_CASE("throws when trying to decode uint64_t", "[uvarint_utils]") {
  std::istringstream ss("\xA0\xA0\xA0\xA0\x7F");
  CHECK_THROWS(uvarint_utils::decode(ss));
}

TEST_CASE("throws EOFException on empty input", "[uvarint_utils]") {
  std::istringstream ss("");
  CHECK_THROWS_AS(uvarint_utils::decode(ss), EOFException);
}

TEST_CASE("more uvarint tests", "[uvarint_utils]") {
  std::vector<uint32_t> values{1, 127, 128, 255, 16384, 190};
  std::vector<std::string> codes
      {"\x01", "\x7F", "\x80\x01", "\xFF\x01", "\x80\x80\x01", "\xbe\x01"};
  REQUIRE(values.size() == codes.size());

  for (size_t i = 0; i < values.size(); ++i) {
    std::ostringstream to;
    uvarint_utils::encode(to, values[i]);
    REQUIRE(to.str() == codes[i]);

    std::istringstream from(codes[i]);
    auto value = uvarint_utils::decode(from);
    REQUIRE(value == values[i]);
  }
}
