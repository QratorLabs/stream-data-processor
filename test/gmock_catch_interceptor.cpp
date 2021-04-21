#include <sstream>

#include <catch2/catch.hpp>

#include "gmock_catch_interceptor.h"

void GmockCatchInterceptor::OnTestPartResult(
    const ::testing::TestPartResult & gmock_assertion_result) {
  std::stringstream failure_place;
  if (gmock_assertion_result.file_name() == nullptr) {
    failure_place << "unknown place";
  } else {
    failure_place << gmock_assertion_result.file_name();
    if (gmock_assertion_result.line_number() != -1) {
      failure_place << ':' << gmock_assertion_result.line_number();
    }
  }

  INFO( "*** Failure in "
            << failure_place.str() << "\n  "
            << gmock_assertion_result.summary() << '\n');
  CHECK_FALSE(gmock_assertion_result.failed()); // inverse logic
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
