#include <catch2/catch.hpp>

#include "gmock_catch_interceptor.h"

void GmockCatchInterceptor::OnTestPartResult(
    const ::testing::TestPartResult & gmock_assertion_result) {
  	printf("%s in %s:%d\n%s\n",
  			gmock_assertion_result.failed() ? "*** Failure" : "Success",
  			gmock_assertion_result.file_name(),
  			gmock_assertion_result.line_number(),
  			gmock_assertion_result.summary());
  REQUIRE_FALSE(gmock_assertion_result.failed()); // inverse logic
}

void GmockCatchInterceptor::OnTestProgramStart(const ::testing::UnitTest& unit_test) {
  //printf("OnTestProgramStart\n");
}
void GmockCatchInterceptor::OnTestIterationStart(const ::testing::UnitTest& unit_test,
                                                   int iteration) {
  //printf("OnTestIterationStart\n");
}
void GmockCatchInterceptor::OnEnvironmentsSetUpStart(
    const ::testing::UnitTest& unit_test) {
  //printf("OnEnvironmentsSetUpStart\n");
}
void GmockCatchInterceptor::OnEnvironmentsSetUpEnd(
    const ::testing::UnitTest& unit_test) {
  //printf("OnEnvironmentsSetUpEnd\n");
}
void GmockCatchInterceptor::OnTestCaseStart(const ::testing::TestCase& test_case) {
  //printf("OnTestCaseStart\n");
}
void GmockCatchInterceptor::OnTestStart(const ::testing::TestInfo& test_info) {
  //printf("OnTestStart\n");
}
void GmockCatchInterceptor::OnTestEnd(const ::testing::TestInfo& test_info) {
  //printf("OnTestEnd\n");
}
void GmockCatchInterceptor::OnTestCaseEnd(const ::testing::TestCase& test_case) {
  //printf("OnTestCaseEnd\n");
}
void GmockCatchInterceptor::OnEnvironmentsTearDownStart(
    const ::testing::UnitTest& unit_test) {
  //printf("OnEnvironmentsTearDownStart\n");
}
void GmockCatchInterceptor::OnEnvironmentsTearDownEnd(
    const ::testing::UnitTest& unit_test) {
  //printf("OnEnvironmentsTearDownEnd\n");
}
void GmockCatchInterceptor::OnTestIterationEnd(const ::testing::UnitTest& unit_test,
                                                 int iteration) {
  //printf("OnTestIterationEnd\n");
}
void GmockCatchInterceptor::OnTestProgramEnd(const ::testing::UnitTest& unit_test) {
  //printf("OnTestProgramEnd\n");
}
