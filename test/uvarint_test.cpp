#include <sstream>

#include <catch2/catch.hpp>

#include "utils/uvarint.h"

TEST_CASE( "encode unsigned varint 300", "[UVarIntCoder]" ) {
  std::ostringstream ss;
  UVarIntCoder::encode(ss, 300);
  REQUIRE( ss.str() == "\xAC\x02" );
}

TEST_CASE( "decode unsigned varint 300", "[UVarIntCoder]" ) {
  std::istringstream ss("\xAC\x02");
  auto value = UVarIntCoder::decode(ss);
  REQUIRE( value == 300 );
}

TEST_CASE( "encode unsigned varint 12", "[UVarIntCoder]" ) {
  std::ostringstream ss;
  UVarIntCoder::encode(ss, 12);
  REQUIRE( ss.str() == "\x0C" );
}

TEST_CASE( "decode unsigned varint 12 as part of data", "[UVarIntCoder]" ) {
  std::istringstream ss("\x0c\x1a\x0a\x08\xb0\xbd\xbc\xf5\x82\xc3\xd1\x9c\x16");
  auto value = UVarIntCoder::decode(ss);
  REQUIRE( value == 12 );
}

TEST_CASE( "throws EOFException when trying to decode sequence with all-first-1", "[UVarIntCoder]" ) {
  std::istringstream ss("\xAC\xAC");
  CHECK_THROWS_AS( UVarIntCoder::decode(ss), EOFException );
}

TEST_CASE( "throws when trying to decode uint64_t", "[UVarIntCoder]" ) {
  std::istringstream ss("\xA0\xA0\xA0\xA0\x7F");
  CHECK_THROWS( UVarIntCoder::decode(ss) );
}

TEST_CASE( "throws EOFException on empty input", "[UVarIntCoder]" ) {
  std::istringstream ss("");
  CHECK_THROWS_AS( UVarIntCoder::decode(ss), EOFException );
}
