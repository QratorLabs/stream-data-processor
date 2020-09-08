#include <sstream>

#include <catch2/catch.hpp>

#include "utils/uvarint.h"

TEST_CASE( "encode unsigned varint 300", "[UVarIntCoder]" ) {
  std::stringstream ss;
  UVarIntCoder::encode(ss, 300);
  REQUIRE( ss.str() == "\xAC\x02" );
}

TEST_CASE( "decode unsigned varint 300", "[UVarIntCoder]" ) {
  std::stringstream ss("\xAC\x02");
  auto value = UVarIntCoder::decode(ss);
  REQUIRE( value == 300 );
}

TEST_CASE( "throws EOFException when trying to decode sequence with all-first-1", "[UVarIntCoder]" ) {
  std::stringstream ss("\xAC\xAC");
  CHECK_THROWS_AS( UVarIntCoder::decode(ss), EOFException );
}

TEST_CASE( "throws when trying to decode uint64_t", "[UVarIntCoder]" ) {
  std::stringstream ss("\xA0\xA0\xA0\xA0\x7F");
  CHECK_THROWS( UVarIntCoder::decode(ss) );
}

TEST_CASE( "throws EOFException on empty input", "[UVarIntCoder]" ) {
  std::stringstream ss("");
  CHECK_THROWS_AS( UVarIntCoder::decode(ss), EOFException );
}
