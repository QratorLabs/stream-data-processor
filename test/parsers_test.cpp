#include <gtest/gtest.h>

#include "utils/parsers/graphite_parser.h"

TEST(GraphiteParserTest, SimpleTest) {
  std::vector<std::string> templates{"measurement.measurement.field.field.region"};
  std::shared_ptr<Parser> parser = std::make_shared<GraphiteParser>(templates, "_");
  auto metric_buffer = std::make_shared<arrow::Buffer>("cpu.usage.idle.percent.eu-east 100");
  arrow::RecordBatchVector record_batch_vector;
  ASSERT_EQ(arrow::Status::OK(), parser->parseRecordBatches(metric_buffer, record_batch_vector));

  ASSERT_EQ(1, record_batch_vector.size());
  ASSERT_EQ(1, record_batch_vector[0]->num_rows());
  ASSERT_EQ(3, record_batch_vector[0]->columns().size());

  ASSERT_NE(nullptr, record_batch_vector[0]->GetColumnByName("measurement"));
  ASSERT_NE(nullptr, record_batch_vector[0]->GetColumnByName("region"));
  ASSERT_NE(nullptr, record_batch_vector[0]->GetColumnByName("idle_percent"));

  auto measurement_result = record_batch_vector[0]->GetColumnByName("measurement")->GetScalar(0);
  ASSERT_EQ(arrow::Status::OK(), measurement_result.status());
  ASSERT_EQ("cpu_usage",
      std::static_pointer_cast<arrow::StringScalar>(measurement_result.ValueOrDie())->value->ToString());

  auto tag_result = record_batch_vector[0]->GetColumnByName("region")->GetScalar(0);
  ASSERT_EQ(arrow::Status::OK(), tag_result.status());
  ASSERT_EQ("eu-east",
      std::static_pointer_cast<arrow::StringScalar>(tag_result.ValueOrDie())->value->ToString());

  auto field_result = record_batch_vector[0]->GetColumnByName("idle_percent")->GetScalar(0);
  ASSERT_EQ(arrow::Status::OK(), field_result.status());
  ASSERT_EQ(100, std::static_pointer_cast<arrow::Int64Scalar>(field_result.ValueOrDie())->value);
}

TEST(GraphiteParserTest, MultipleEndingTest) {
  std::vector<std::string> templates{"region.measurement*"};
  std::shared_ptr<Parser> parser = std::make_shared<GraphiteParser>(templates);
  auto metric_buffer = std::make_shared<arrow::Buffer>("us.cpu.load 100");
  arrow::RecordBatchVector record_batch_vector;
  ASSERT_EQ(arrow::Status::OK(), parser->parseRecordBatches(metric_buffer, record_batch_vector));

  ASSERT_EQ(1, record_batch_vector.size());
  ASSERT_EQ(1, record_batch_vector[0]->num_rows());
  ASSERT_EQ(3, record_batch_vector[0]->columns().size());

  ASSERT_NE(nullptr, record_batch_vector[0]->GetColumnByName("measurement"));
  ASSERT_NE(nullptr, record_batch_vector[0]->GetColumnByName("region"));
  ASSERT_NE(nullptr, record_batch_vector[0]->GetColumnByName("value"));

  auto measurement_result = record_batch_vector[0]->GetColumnByName("measurement")->GetScalar(0);
  ASSERT_EQ(arrow::Status::OK(), measurement_result.status());
  ASSERT_EQ("cpu.load",
            std::static_pointer_cast<arrow::StringScalar>(measurement_result.ValueOrDie())->value->ToString());

  auto tag_result = record_batch_vector[0]->GetColumnByName("region")->GetScalar(0);
  ASSERT_EQ(arrow::Status::OK(), tag_result.status());
  ASSERT_EQ("us",
            std::static_pointer_cast<arrow::StringScalar>(tag_result.ValueOrDie())->value->ToString());

  auto field_result = record_batch_vector[0]->GetColumnByName("value")->GetScalar(0);
  ASSERT_EQ(arrow::Status::OK(), field_result.status());
  ASSERT_EQ(100, std::static_pointer_cast<arrow::Int64Scalar>(field_result.ValueOrDie())->value);
}

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}