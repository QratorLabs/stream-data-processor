#include <sstream>

#include <catch2/catch.hpp>

#include "gmock_catch_interceptor.h"

void GmockCatchInterceptor::OnTestPartResult(
    const ::testing::TestPartResult & gmock_assertion_result) {
  try {
    INFO( "*** Failure in "
              << gmock_assertion_result.file_name() << ':'
              << gmock_assertion_result.line_number() << "\n  "
              << gmock_assertion_result.summary() << '\n');
    REQUIRE_FALSE(gmock_assertion_result.failed()); // inverse logic
  } catch (Catch::TestFailureException) {

  }
}

void GmockCatchInterceptor::OnTestProgramStart(const ::testing::UnitTest& unit_test) {

}
void GmockCatchInterceptor::OnTestIterationStart(const ::testing::UnitTest& unit_test,
                                                   int iteration) {

}
void GmockCatchInterceptor::OnEnvironmentsSetUpStart(
    const ::testing::UnitTest& unit_test) {

}
void GmockCatchInterceptor::OnEnvironmentsSetUpEnd(
    const ::testing::UnitTest& unit_test) {

}
void GmockCatchInterceptor::OnTestCaseStart(const ::testing::TestCase& test_case) {

}
void GmockCatchInterceptor::OnTestStart(const ::testing::TestInfo& test_info) {

}
void GmockCatchInterceptor::OnTestEnd(const ::testing::TestInfo& test_info) {

}
void GmockCatchInterceptor::OnTestCaseEnd(const ::testing::TestCase& test_case) {

}
void GmockCatchInterceptor::OnEnvironmentsTearDownStart(
    const ::testing::UnitTest& unit_test) {

}
void GmockCatchInterceptor::OnEnvironmentsTearDownEnd(
    const ::testing::UnitTest& unit_test) {

}
void GmockCatchInterceptor::OnTestIterationEnd(const ::testing::UnitTest& unit_test,
                                                 int iteration) {

}
void GmockCatchInterceptor::OnTestProgramEnd(const ::testing::UnitTest& unit_test) {

}
