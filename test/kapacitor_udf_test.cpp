#include <catch2/catch.hpp>
#include "gmock/gmock.h"

#include "kapacitor_udf/agent.h"

#include "udf.pb.h"

class MockAgent : public IAgent {
 public:
  MOCK_METHOD(void, start, (), (override));
  MOCK_METHOD(void, wait, (), (override));
  MOCK_METHOD(void, writeResponse, (const agent::Response& response), (override));
};

TEST_CASE( "test mock", "[Agent]") {
  MockAgent mock_agent;
  EXPECT_CALL(mock_agent, start()).Times(::testing::AtLeast(1));
  mock_agent.start();
}
