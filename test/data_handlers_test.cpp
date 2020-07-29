#include <memory>
#include <vector>

#include <arrow/api.h>
#include <gandiva/tree_expr_builder.h>

#include <gtest/gtest.h>

#include "data_handlers/data_handlers.h"
#include "help.h"
#include "utils/serializer.h"

TEST(FilterHandlerTest, SimpleTest) {
  auto field = arrow::field("field_name", arrow::int64());
  auto schema = arrow::schema({field});

  arrow::Int64Builder array_builder;
  ASSERT_TRUE(arrowAssertNotOk(array_builder.Append(0)));
  ASSERT_TRUE(arrowAssertNotOk(array_builder.Append(1)));
  std::shared_ptr<arrow::Array> array;
  ASSERT_TRUE(arrowAssertNotOk(array_builder.Finish(&array)));
  auto record_batch = arrow::RecordBatch::Make(schema, 2, {array});

  auto equal_node = gandiva::TreeExprBuilder::MakeFunction("equal",{
      gandiva::TreeExprBuilder::MakeLiteral(int64_t(0)),
      gandiva::TreeExprBuilder::MakeField(field)
    }, arrow::boolean());
  std::vector<gandiva::ConditionPtr> conditions{gandiva::TreeExprBuilder::MakeCondition(equal_node)};
  std::shared_ptr<DataHandler> filter_handler = std::make_shared<FilterHandler>(schema, conditions);

  std::shared_ptr<arrow::Buffer> source, target;
  ASSERT_TRUE(arrowAssertNotOk(Serializer::serializeRecordBatches(schema, {record_batch}, &source)));

  ASSERT_TRUE(arrowAssertNotOk(filter_handler->handle(source, &target)));

  arrow::RecordBatchVector record_batch_vector;
  ASSERT_TRUE(arrowAssertNotOk(Serializer::deserializeRecordBatches(target, &record_batch_vector)));

  ASSERT_EQ(1, record_batch_vector.size());
  checkSize(record_batch_vector[0], 1, 1);
  checkColumnsArePresent(record_batch_vector[0], {"field_name"});
  checkValue<int64_t, arrow::Int64Scalar>(0, record_batch_vector[0],
      "field_name", 0);
}

TEST(GroupHandlerTest, SimpleTest) {
  auto field = arrow::field("field_name", arrow::int64());
  auto schema = arrow::schema({field});

  arrow::Int64Builder array_builder;
  ASSERT_TRUE(arrowAssertNotOk(array_builder.Append(0)));
  ASSERT_TRUE(arrowAssertNotOk(array_builder.Append(1)));
  std::shared_ptr<arrow::Array> array;
  ASSERT_TRUE(arrowAssertNotOk(array_builder.Finish(&array)));
  auto record_batch = arrow::RecordBatch::Make(schema, 2, {array});

  std::vector<std::string> grouping_columns{"field_name"};
  std::shared_ptr<DataHandler> filter_handler = std::make_shared<GroupHandler>(std::move(grouping_columns));

  std::shared_ptr<arrow::Buffer> source, target;
  ASSERT_TRUE(arrowAssertNotOk(Serializer::serializeRecordBatches(schema, {record_batch}, &source)));

  ASSERT_TRUE(arrowAssertNotOk(filter_handler->handle(source, &target)));

  arrow::RecordBatchVector record_batch_vector;
  ASSERT_TRUE(arrowAssertNotOk(Serializer::deserializeRecordBatches(target, &record_batch_vector)));

  ASSERT_EQ(2, record_batch_vector.size());
  checkSize(record_batch_vector[0], 1, 1);
  checkSize(record_batch_vector[1], 1, 1);
  checkColumnsArePresent(record_batch_vector[0], {"field_name"});
  checkColumnsArePresent(record_batch_vector[1], {"field_name"});
  if (!equals<int64_t, arrow::Int64Scalar>(0, record_batch_vector[0],
      "field_name", 0)) {
    std::swap(record_batch_vector[0], record_batch_vector[1]);
  }

  checkValue<int64_t, arrow::Int64Scalar>(0, record_batch_vector[0],
                                          "field_name", 0);
  checkValue<int64_t, arrow::Int64Scalar>(1, record_batch_vector[1],
                                          "field_name", 0);
}

TEST(DefaultHandlerTest, EmptyTest) {
  auto schema = arrow::schema({arrow::field("field", arrow::null())});

  DefaultHandler::DefaultHandlerOptions options{
      {{"int64_field", 42}},
      {{"double_field", 3.14}},
      {{"string_field", "Hello, world!"}}
  };
  std::shared_ptr<DataHandler> default_handler = std::make_shared<DefaultHandler>(std::move(options));

  arrow::NullBuilder builder;
  std::shared_ptr<arrow::Array> array;
  ASSERT_TRUE(arrowAssertNotOk(builder.Finish(&array)));
  auto record_batch = arrow::RecordBatch::Make(schema, 0, {array});

  std::shared_ptr<arrow::Buffer> source, target;
  ASSERT_TRUE(arrowAssertNotOk(Serializer::serializeRecordBatches(schema, {record_batch}, &source)));

  ASSERT_TRUE(arrowAssertNotOk(default_handler->handle(source, &target)));

  arrow::RecordBatchVector record_batch_vector;
  ASSERT_TRUE(arrowAssertNotOk(Serializer::deserializeRecordBatches(target, &record_batch_vector)));

  ASSERT_EQ(1, record_batch_vector.size());
  checkSize(record_batch_vector[0], 0, 4);
  checkColumnsArePresent(record_batch_vector[0], {
    "field",
    "int64_field",
    "double_field",
    "string_field"
  });
}

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
