#include <deque>
#include <memory>
#include <vector>

#include <arrow/api.h>

#include <gtest/gtest.h>

#include "period_handlers/period_handlers.h"
#include "help.h"
#include "utils/serializer.h"

TEST(JoinHandlerTest, SimpleTest) {
  auto ts_field = arrow::field("time", arrow::timestamp(arrow::TimeUnit::SECOND));
  auto tag_field = arrow::field("tag", arrow::utf8());

  auto field_1_field = arrow::field("field_1", arrow::int64());
  auto schema_1 = arrow::schema({ts_field, tag_field, field_1_field});

  auto field_2_field = arrow::field("field_2", arrow::float64());
  auto schema_2 = arrow::schema({ts_field, tag_field, field_2_field});

  arrow::TimestampBuilder ts_builder(arrow::timestamp(arrow::TimeUnit::SECOND), arrow::default_memory_pool());
  ASSERT_TRUE(arrowAssertNotOk(ts_builder.Append(100)));
  std::shared_ptr<arrow::Array> ts_array_1;
  ASSERT_TRUE(arrowAssertNotOk(ts_builder.Finish(&ts_array_1)));

  ASSERT_TRUE(arrowAssertNotOk(ts_builder.Append(100)));
  std::shared_ptr<arrow::Array> ts_array_2;
  ASSERT_TRUE(arrowAssertNotOk(ts_builder.Finish(&ts_array_2)));

  arrow::StringBuilder tag_builder;
  ASSERT_TRUE(arrowAssertNotOk(tag_builder.Append("tag_value")));
  std::shared_ptr<arrow::Array> tag_array_1;
  ASSERT_TRUE(arrowAssertNotOk(tag_builder.Finish(&tag_array_1)));

  ASSERT_TRUE(arrowAssertNotOk(tag_builder.Append("tag_value")));
  std::shared_ptr<arrow::Array> tag_array_2;
  ASSERT_TRUE(arrowAssertNotOk(tag_builder.Finish(&tag_array_2)));

  arrow::Int64Builder field_1_builder;
  ASSERT_TRUE(arrowAssertNotOk(field_1_builder.Append(42)));
  std::shared_ptr<arrow::Array> field_1_array;
  ASSERT_TRUE(arrowAssertNotOk(field_1_builder.Finish(&field_1_array)));

  arrow::DoubleBuilder field_2_builder;
  ASSERT_TRUE(arrowAssertNotOk(field_2_builder.Append(3.14)));
  std::shared_ptr<arrow::Array> field_2_array;
  ASSERT_TRUE(arrowAssertNotOk(field_2_builder.Finish(&field_2_array)));

  auto record_batch_1 = arrow::RecordBatch::Make(schema_1, 1, {ts_array_1, tag_array_1, field_1_array});
  auto record_batch_2 = arrow::RecordBatch::Make(schema_2, 1, {ts_array_2, tag_array_2, field_2_array});

  std::deque<std::shared_ptr<arrow::Buffer>> buffers(2);
  std::shared_ptr<arrow::Buffer> result;
  ASSERT_TRUE(arrowAssertNotOk(Serializer::serializeRecordBatches(schema_1, {record_batch_1}, &buffers[0])));
  ASSERT_TRUE(arrowAssertNotOk(Serializer::serializeRecordBatches(schema_2, {record_batch_2}, &buffers[1])));

  std::vector<std::string> join_on_columns{"tag"};
  std::shared_ptr<PeriodHandler> handler = std::make_shared<JoinHandler>(std::move(join_on_columns), "time");

  ASSERT_TRUE(arrowAssertNotOk(handler->handle(buffers, result)));

  arrow::RecordBatchVector record_batch_vector;
  ASSERT_TRUE(arrowAssertNotOk(Serializer::deserializeRecordBatches(result, &record_batch_vector)));

  ASSERT_EQ(1, record_batch_vector.size());
  checkSize(record_batch_vector[0], 1, 4);
  checkColumnsArePresent(record_batch_vector[0], {
    "time", "tag", "field_1", "field_2"
  });
  checkValue<int64_t, arrow::Int64Scalar>(100, record_batch_vector[0],
                                          "time", 0);
  checkValue<std::string, arrow::StringScalar>("tag_value", record_batch_vector[0],
                                          "tag", 0);
  checkValue<int64_t, arrow::Int64Scalar>(42, record_batch_vector[0],
                                          "field_1", 0);
  checkValue<double, arrow::DoubleScalar>(3.14, record_batch_vector[0],
                                          "field_2", 0);
}

TEST(JoinHandlerTest, MissedRecordsTest) {
  auto ts_field = arrow::field("time", arrow::timestamp(arrow::TimeUnit::SECOND));
  auto tag_field = arrow::field("tag", arrow::utf8());

  auto field_1_field = arrow::field("field_1", arrow::int64());
  auto schema_1 = arrow::schema({ts_field, tag_field, field_1_field});

  auto field_2_field = arrow::field("field_2", arrow::float64());
  auto schema_2 = arrow::schema({ts_field, tag_field, field_2_field});

  arrow::TimestampBuilder ts_builder(arrow::timestamp(arrow::TimeUnit::SECOND), arrow::default_memory_pool());
  ASSERT_TRUE(arrowAssertNotOk(ts_builder.Append(105)));
  ASSERT_TRUE(arrowAssertNotOk(ts_builder.Append(110)));
  std::shared_ptr<arrow::Array> ts_array_1;
  ASSERT_TRUE(arrowAssertNotOk(ts_builder.Finish(&ts_array_1)));

  ASSERT_TRUE(arrowAssertNotOk(ts_builder.Append(110)));
  std::shared_ptr<arrow::Array> ts_array_2;
  ASSERT_TRUE(arrowAssertNotOk(ts_builder.Finish(&ts_array_2)));

  arrow::StringBuilder tag_builder;
  ASSERT_TRUE(arrowAssertNotOk(tag_builder.Append("tag_value")));
  ASSERT_TRUE(arrowAssertNotOk(tag_builder.Append("other_tag_value")));
  std::shared_ptr<arrow::Array> tag_array_1;
  ASSERT_TRUE(arrowAssertNotOk(tag_builder.Finish(&tag_array_1)));

  ASSERT_TRUE(arrowAssertNotOk(tag_builder.Append("tag_value")));
  std::shared_ptr<arrow::Array> tag_array_2;
  ASSERT_TRUE(arrowAssertNotOk(tag_builder.Finish(&tag_array_2)));

  arrow::Int64Builder field_1_builder;
  ASSERT_TRUE(arrowAssertNotOk(field_1_builder.Append(43)));
  ASSERT_TRUE(arrowAssertNotOk(field_1_builder.Append(44)));
  std::shared_ptr<arrow::Array> field_1_array;
  ASSERT_TRUE(arrowAssertNotOk(field_1_builder.Finish(&field_1_array)));

  arrow::DoubleBuilder field_2_builder;
  ASSERT_TRUE(arrowAssertNotOk(field_2_builder.Append(2.71)));
  std::shared_ptr<arrow::Array> field_2_array;
  ASSERT_TRUE(arrowAssertNotOk(field_2_builder.Finish(&field_2_array)));

  auto record_batch_1 = arrow::RecordBatch::Make(schema_1, 2, {ts_array_1, tag_array_1, field_1_array});
  auto record_batch_2 = arrow::RecordBatch::Make(schema_2, 1, {ts_array_2, tag_array_2, field_2_array});

  std::deque<std::shared_ptr<arrow::Buffer>> buffers(2);
  std::shared_ptr<arrow::Buffer> result;
  ASSERT_TRUE(arrowAssertNotOk(Serializer::serializeRecordBatches(schema_1, {record_batch_1}, &buffers[0])));
  ASSERT_TRUE(arrowAssertNotOk(Serializer::serializeRecordBatches(schema_2, {record_batch_2}, &buffers[1])));

  std::vector<std::string> join_on_columns{"tag"};
  std::shared_ptr<PeriodHandler> handler = std::make_shared<JoinHandler>(std::move(join_on_columns), "time");

  ASSERT_TRUE(arrowAssertNotOk(handler->handle(buffers, result)));

  arrow::RecordBatchVector record_batch_vector;
  ASSERT_TRUE(arrowAssertNotOk(Serializer::deserializeRecordBatches(result, &record_batch_vector)));

  ASSERT_EQ(1, record_batch_vector.size());
  checkSize(record_batch_vector[0], 3, 4);
  checkColumnsArePresent(record_batch_vector[0], {
      "time", "tag", "field_1", "field_2"
  });
  checkValue<int64_t, arrow::Int64Scalar>(105, record_batch_vector[0],
                                          "time", 0);
  checkValue<std::string, arrow::StringScalar>("tag_value", record_batch_vector[0],
                                               "tag", 0);
  checkValue<int64_t, arrow::Int64Scalar>(43, record_batch_vector[0],
                                          "field_1", 0);
  checkIsNull(record_batch_vector[0], "field_2", 0);

  checkValue<int64_t, arrow::Int64Scalar>(110, record_batch_vector[0],
                                          "time", 1);
  checkIsValid(record_batch_vector[0],"tag", 1);

  checkValue<int64_t, arrow::Int64Scalar>(110, record_batch_vector[0],
                                          "time", 2);
  checkIsValid(record_batch_vector[0],"tag", 2);
}

TEST(JoinHandlerTest, ToleranceTest) {
  auto ts_field = arrow::field("time", arrow::timestamp(arrow::TimeUnit::SECOND));
  auto tag_field = arrow::field("tag", arrow::utf8());

  auto field_1_field = arrow::field("field_1", arrow::int64());
  auto schema_1 = arrow::schema({ts_field, tag_field, field_1_field});

  auto field_2_field = arrow::field("field_2", arrow::float64());
  auto schema_2 = arrow::schema({ts_field, tag_field, field_2_field});

  arrow::TimestampBuilder ts_builder(arrow::timestamp(arrow::TimeUnit::SECOND), arrow::default_memory_pool());
  ASSERT_TRUE(arrowAssertNotOk(ts_builder.Append(100)));
  std::shared_ptr<arrow::Array> ts_array_1;
  ASSERT_TRUE(arrowAssertNotOk(ts_builder.Finish(&ts_array_1)));

  ASSERT_TRUE(arrowAssertNotOk(ts_builder.Append(103)));
  std::shared_ptr<arrow::Array> ts_array_2;
  ASSERT_TRUE(arrowAssertNotOk(ts_builder.Finish(&ts_array_2)));

  arrow::StringBuilder tag_builder;
  ASSERT_TRUE(arrowAssertNotOk(tag_builder.Append("tag_value")));
  std::shared_ptr<arrow::Array> tag_array_1;
  ASSERT_TRUE(arrowAssertNotOk(tag_builder.Finish(&tag_array_1)));

  ASSERT_TRUE(arrowAssertNotOk(tag_builder.Append("tag_value")));
  std::shared_ptr<arrow::Array> tag_array_2;
  ASSERT_TRUE(arrowAssertNotOk(tag_builder.Finish(&tag_array_2)));

  arrow::Int64Builder field_1_builder;
  ASSERT_TRUE(arrowAssertNotOk(field_1_builder.Append(42)));
  std::shared_ptr<arrow::Array> field_1_array;
  ASSERT_TRUE(arrowAssertNotOk(field_1_builder.Finish(&field_1_array)));

  arrow::DoubleBuilder field_2_builder;
  ASSERT_TRUE(arrowAssertNotOk(field_2_builder.Append(3.14)));
  std::shared_ptr<arrow::Array> field_2_array;
  ASSERT_TRUE(arrowAssertNotOk(field_2_builder.Finish(&field_2_array)));

  auto record_batch_1 = arrow::RecordBatch::Make(schema_1, 1, {ts_array_1, tag_array_1, field_1_array});
  auto record_batch_2 = arrow::RecordBatch::Make(schema_2, 1, {ts_array_2, tag_array_2, field_2_array});

  std::deque<std::shared_ptr<arrow::Buffer>> buffers(2);
  std::shared_ptr<arrow::Buffer> result;
  ASSERT_TRUE(arrowAssertNotOk(Serializer::serializeRecordBatches(schema_1, {record_batch_1}, &buffers[0])));
  ASSERT_TRUE(arrowAssertNotOk(Serializer::serializeRecordBatches(schema_2, {record_batch_2}, &buffers[1])));

  std::vector<std::string> join_on_columns{"tag"};
  std::shared_ptr<PeriodHandler> handler = std::make_shared<JoinHandler>(std::move(join_on_columns), "time", 5);

  ASSERT_TRUE(arrowAssertNotOk(handler->handle(buffers, result)));

  arrow::RecordBatchVector record_batch_vector;
  ASSERT_TRUE(arrowAssertNotOk(Serializer::deserializeRecordBatches(result, &record_batch_vector)));

  ASSERT_EQ(1, record_batch_vector.size());
  checkSize(record_batch_vector[0], 1, 4);
  checkColumnsArePresent(record_batch_vector[0], {
      "time", "tag", "field_1", "field_2"
  });
  checkIsValid(record_batch_vector[0],"time", 0);
  checkValue<std::string, arrow::StringScalar>("tag_value", record_batch_vector[0],
                                               "tag", 0);
  checkValue<int64_t, arrow::Int64Scalar>(42, record_batch_vector[0],
                                          "field_1", 0);
  checkValue<double, arrow::DoubleScalar>(3.14, record_batch_vector[0],
                                          "field_2", 0);
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
