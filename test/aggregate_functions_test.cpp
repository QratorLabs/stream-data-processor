#include <memory>

#include <arrow/api.h>

#include <gtest/gtest.h>

#include "data_handlers/aggregate_functions/aggregate_functions.h"
#include "test_help.h"

TEST(MeanAggregateFunctionTest, SimpleTest) {
  std::shared_ptr<AggregateFunction> mean_function = std::make_shared<MeanAggregateFunction>();

  auto field = arrow::field("field_name", arrow::int64());
  auto schema = arrow::schema({field});

  arrow::Int64Builder array_builder;
  ASSERT_TRUE(arrowAssertNotOk(array_builder.Append(0)));
  ASSERT_TRUE(arrowAssertNotOk(array_builder.Append(2)));
  std::shared_ptr<arrow::Array> array;
  ASSERT_TRUE(arrowAssertNotOk(array_builder.Finish(&array)));
  auto record_batch = arrow::RecordBatch::Make(schema, 2, {array});

  std::shared_ptr<arrow::Scalar> result;
  ASSERT_TRUE(arrowAssertNotOk(mean_function->aggregate(record_batch, "field_name", &result)));
  ASSERT_EQ(1, std::static_pointer_cast<arrow::DoubleScalar>(result)->value);
}

TEST(MeanAggregateFunctionTest, NonIntegerResultTest) {
  std::shared_ptr<AggregateFunction> mean_function = std::make_shared<MeanAggregateFunction>();

  auto field = arrow::field("field_name", arrow::int64());
  auto schema = arrow::schema({field});

  arrow::Int64Builder array_builder;
  ASSERT_TRUE(arrowAssertNotOk(array_builder.Append(0)));
  ASSERT_TRUE(arrowAssertNotOk(array_builder.Append(1)));
  std::shared_ptr<arrow::Array> array;
  ASSERT_TRUE(arrowAssertNotOk(array_builder.Finish(&array)));
  auto record_batch = arrow::RecordBatch::Make(schema, 2, {array});

  std::shared_ptr<arrow::Scalar> result;
  ASSERT_TRUE(arrowAssertNotOk(mean_function->aggregate(record_batch, "field_name", &result)));
  ASSERT_EQ(0.5, std::static_pointer_cast<arrow::DoubleScalar>(result)->value);
}
