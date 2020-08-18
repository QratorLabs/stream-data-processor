#include <memory>
#include <vector>

#include <arrow/api.h>
#include <gandiva/tree_expr_builder.h>

#include <catch2/catch.hpp>

#include "data_handlers/data_handlers.h"
#include "test_help.h"
#include "utils/serializer.h"

TEST_CASE( "filter one of two integers based on equal function", "[FilterHandler]" ) {
  auto field = arrow::field("field_name", arrow::int64());
  auto schema = arrow::schema({field});

  arrow::Int64Builder array_builder;
  arrowAssertNotOk(array_builder.Append(0));
  arrowAssertNotOk(array_builder.Append(1));
  std::shared_ptr<arrow::Array> array;
  arrowAssertNotOk(array_builder.Finish(&array));
  auto record_batch = arrow::RecordBatch::Make(schema, 2, {array});

  auto equal_node = gandiva::TreeExprBuilder::MakeFunction("equal",{
      gandiva::TreeExprBuilder::MakeLiteral(int64_t(0)),
      gandiva::TreeExprBuilder::MakeField(field)
    }, arrow::boolean());
  std::vector<gandiva::ConditionPtr> conditions{gandiva::TreeExprBuilder::MakeCondition(equal_node)};
  std::shared_ptr<DataHandler> filter_handler = std::make_shared<FilterHandler>(std::move(conditions));

  std::shared_ptr<arrow::Buffer> source, target;
  arrowAssertNotOk(Serializer::serializeRecordBatches(schema, {record_batch}, &source));

  arrowAssertNotOk(filter_handler->handle(source, &target));

  arrow::RecordBatchVector record_batch_vector;
  arrowAssertNotOk(Serializer::deserializeRecordBatches(target, &record_batch_vector));

  REQUIRE( record_batch_vector.size() == 1 );
  checkSize(record_batch_vector[0], 1, 1);
  checkColumnsArePresent(record_batch_vector[0], {"field_name"});
  checkValue<int64_t, arrow::Int64Scalar>(0, record_batch_vector[0],
      "field_name", 0);
}

TEST_CASE ( "filter one of two strings based on equal function", "[FilterHandler]" ) {
  auto field = arrow::field("field_name", arrow::utf8());
  auto schema = arrow::schema({field});

  arrow::StringBuilder array_builder;
  arrowAssertNotOk(array_builder.Append("hello"));
  arrowAssertNotOk(array_builder.Append("world"));
  std::shared_ptr<arrow::Array> array;
  arrowAssertNotOk(array_builder.Finish(&array));
  auto record_batch = arrow::RecordBatch::Make(schema, 2, {array});

  auto equal_node = gandiva::TreeExprBuilder::MakeFunction("equal",{
      gandiva::TreeExprBuilder::MakeStringLiteral("hello"),
      gandiva::TreeExprBuilder::MakeField(field)
  }, arrow::boolean());
  std::vector<gandiva::ConditionPtr> conditions{gandiva::TreeExprBuilder::MakeCondition(equal_node)};
  std::shared_ptr<DataHandler> filter_handler = std::make_shared<FilterHandler>(std::move(conditions));

  std::shared_ptr<arrow::Buffer> source, target;
  arrowAssertNotOk(Serializer::serializeRecordBatches(schema, {record_batch}, &source));

  arrowAssertNotOk(filter_handler->handle(source, &target));

  arrow::RecordBatchVector record_batch_vector;
  arrowAssertNotOk(Serializer::deserializeRecordBatches(target, &record_batch_vector));

  REQUIRE( record_batch_vector.size() == 1 );
  checkSize(record_batch_vector[0], 1, 1);
  checkColumnsArePresent(record_batch_vector[0], {"field_name"});
  checkValue<std::string, arrow::StringScalar>("hello", record_batch_vector[0],
                                          "field_name", 0);
}

TEST_CASE ( "split one record batch to separate ones by grouping on column with different values", "[GroupHandler]") {
  auto field = arrow::field("field_name", arrow::int64());
  auto schema = arrow::schema({field});

  arrow::Int64Builder array_builder;
  arrowAssertNotOk(array_builder.Append(0));
  arrowAssertNotOk(array_builder.Append(1));
  std::shared_ptr<arrow::Array> array;
  arrowAssertNotOk(array_builder.Finish(&array));
  auto record_batch = arrow::RecordBatch::Make(schema, 2, {array});

  std::vector<std::string> grouping_columns{"field_name"};
  std::shared_ptr<DataHandler> filter_handler = std::make_shared<GroupHandler>(std::move(grouping_columns));

  std::shared_ptr<arrow::Buffer> source, target;
  arrowAssertNotOk(Serializer::serializeRecordBatches(schema, {record_batch}, &source));

  arrowAssertNotOk(filter_handler->handle(source, &target));

  arrow::RecordBatchVector record_batch_vector;
  arrowAssertNotOk(Serializer::deserializeRecordBatches(target, &record_batch_vector));

  REQUIRE( record_batch_vector.size() == 2 );
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

TEST_CASE( "add new columns to empty record batch with different schema", "[DefaultHandler]") {
  auto schema = arrow::schema({arrow::field("field", arrow::null())});

  DefaultHandler::DefaultHandlerOptions options{
      {{"int64_field", 42}},
      {{"double_field", 3.14}},
      {{"string_field", "Hello, world!"}}
  };
  std::shared_ptr<DataHandler> default_handler = std::make_shared<DefaultHandler>(std::move(options));

  arrow::NullBuilder builder;
  std::shared_ptr<arrow::Array> array;
  arrowAssertNotOk(builder.Finish(&array));
  auto record_batch = arrow::RecordBatch::Make(schema, 0, {array});

  std::shared_ptr<arrow::Buffer> source, target;
  arrowAssertNotOk(Serializer::serializeRecordBatches(schema, {record_batch}, &source));

  arrowAssertNotOk(default_handler->handle(source, &target));

  arrow::RecordBatchVector record_batch_vector;
  arrowAssertNotOk(Serializer::deserializeRecordBatches(target, &record_batch_vector));

  REQUIRE( record_batch_vector.size() == 1 );
  checkSize(record_batch_vector[0], 0, 4);
  checkColumnsArePresent(record_batch_vector[0], {
    "field",
    "int64_field",
    "double_field",
    "string_field"
  });
}

TEST_CASE( "add new columns with default values to record batch with different schema", "[DefaultHandler]" ) {
  auto schema = arrow::schema({arrow::field("field", arrow::null())});

  DefaultHandler::DefaultHandlerOptions options{
      {{"int64_field", 42}},
      {{"double_field", 3.14}},
      {{"string_field", "Hello, world!"}}
  };
  std::shared_ptr<DataHandler> default_handler = std::make_shared<DefaultHandler>(std::move(options));

  arrow::NullBuilder builder;
  arrowAssertNotOk(builder.AppendNull());
  std::shared_ptr<arrow::Array> array;
  arrowAssertNotOk(builder.Finish(&array));
  auto record_batch = arrow::RecordBatch::Make(schema, 1, {array});

  std::shared_ptr<arrow::Buffer> source, target;
  arrowAssertNotOk(Serializer::serializeRecordBatches(schema, {record_batch}, &source));

  arrowAssertNotOk(default_handler->handle(source, &target));

  arrow::RecordBatchVector record_batch_vector;
  arrowAssertNotOk(Serializer::deserializeRecordBatches(target, &record_batch_vector));

  REQUIRE( record_batch_vector.size() == 1 );
  checkSize(record_batch_vector[0], 1, 4);
  checkColumnsArePresent(record_batch_vector[0], {
      "field",
      "int64_field",
      "double_field",
      "string_field"
  });
  checkValue<int64_t, arrow::Int64Scalar>(42, record_batch_vector[0],
                                          "int64_field", 0);
  checkValue<double, arrow::DoubleScalar>(3.14, record_batch_vector[0],
                                          "double_field", 0);
  checkValue<std::string, arrow::StringScalar>("Hello, world!", record_batch_vector[0],
                                          "string_field", 0);
}
