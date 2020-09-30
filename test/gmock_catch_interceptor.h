#pragma once

#include <gmock/gmock.h>

class GmockCatchInterceptor : public ::testing::EmptyTestEventListener {
 public:
  GmockCatchInterceptor() = default;
  ~GmockCatchInterceptor() override = default;
  // Called after a failed assertion or a SUCCEED() invocation.
  void OnTestPartResult(::testing::TestPartResult const & test_part_result) override;

  void OnTestProgramStart(const ::testing::UnitTest& /*unit_test*/) override;
  void OnTestIterationStart(const ::testing::UnitTest& /*unit_test*/, int /*iteration*/) override;
  void OnEnvironmentsSetUpStart(const ::testing::UnitTest& /*unit_test*/) override;
  void OnEnvironmentsSetUpEnd(const ::testing::UnitTest& /*unit_test*/) override;
  void OnTestCaseStart(const ::testing::TestCase& /*test_case*/) override;
  void OnTestStart(const ::testing::TestInfo& /*test_info*/) override;
  void OnTestEnd(const ::testing::TestInfo& /*test_info*/) override;
  void OnTestCaseEnd(const ::testing::TestCase& /*test_case*/) override;
  void OnEnvironmentsTearDownStart(const ::testing::UnitTest& /*unit_test*/) override;
  void OnEnvironmentsTearDownEnd(const ::testing::UnitTest& /*unit_test*/) override;
  void OnTestIterationEnd(const ::testing::UnitTest& /*unit_test*/, int /*iteration*/) override;
  void OnTestProgramEnd(const ::testing::UnitTest& /*unit_test*/) override;
};
