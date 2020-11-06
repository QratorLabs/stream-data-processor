#include <deque>
#include <memory>
#include <vector>

#include <arrow/api.h>
#include <gandiva/tree_expr_builder.h>

#include <catch2/catch.hpp>

#include "metadata/column_typing.h"
#include "metadata/grouping.h"
#include "record_batch_handlers/record_batch_handlers.h"
#include "test_help.h"
#include "utils/serialize_utils.h"

using namespace stream_data_processor;

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
  std::shared_ptr<RecordBatchHandler> filter_handler = std::make_shared<FilterHandler>(std::move(conditions));

  arrow::RecordBatchVector result;
  arrowAssertNotOk(filter_handler->handle({record_batch}, &result));

  REQUIRE( result.size() == 1 );
  checkSize(result[0], 1, 1);
  checkColumnsArePresent(result[0], {"field_name"});
  checkValue<int64_t, arrow::Int64Scalar>(0, result[0],
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
  std::shared_ptr<RecordBatchHandler> filter_handler = std::make_shared<FilterHandler>(std::move(conditions));

  arrow::RecordBatchVector result;
  arrowAssertNotOk(filter_handler->handle({record_batch}, &result));

  REQUIRE( result.size() == 1 );
  checkSize(result[0], 1, 1);
  checkColumnsArePresent(result[0], {"field_name"});
  checkValue<std::string, arrow::StringScalar>("hello", result[0],
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
  std::shared_ptr<RecordBatchHandler> filter_handler = std::make_shared<GroupHandler>(std::move(grouping_columns));

  arrow::RecordBatchVector result;
  arrowAssertNotOk(filter_handler->handle({record_batch}, &result));

  REQUIRE( result.size() == 2 );
  checkSize(result[0], 1, 1);
  checkSize(result[1], 1, 1);
  checkColumnsArePresent(result[0], {"field_name"});
  checkColumnsArePresent(result[1], {"field_name"});
  if (!equals<int64_t, arrow::Int64Scalar>(0, result[0],
      "field_name", 0)) {
    std::swap(result[0], result[1]);
  }

  checkValue<int64_t, arrow::Int64Scalar>(0, result[0],
                                          "field_name", 0);
  checkValue<int64_t, arrow::Int64Scalar>(1, result[1],
                                          "field_name", 0);

  REQUIRE(metadata::extractGroupMetadata(result[0]) !=
            metadata::extractGroupMetadata(result[1]) );
}

TEST_CASE( "add new columns to empty record batch with different schema", "[DefaultHandler]") {
  auto schema = arrow::schema({arrow::field("field", arrow::null())});

  DefaultHandler::DefaultHandlerOptions options{
      {{"int64_field", {42}}},
      {{"double_field", {3.14}}},
      {{"string_field", {"Hello, world!"}}}
  };
  std::shared_ptr<RecordBatchHandler> default_handler = std::make_shared<DefaultHandler>(std::move(options));

  arrow::NullBuilder builder;
  std::shared_ptr<arrow::Array> array;
  arrowAssertNotOk(builder.Finish(&array));
  auto record_batch = arrow::RecordBatch::Make(schema, 0, {array});

  arrow::RecordBatchVector result;
  arrowAssertNotOk(default_handler->handle({record_batch}, &result));

  REQUIRE( result.size() == 1 );
  checkSize(result[0], 0, 4);
  checkColumnsArePresent(result[0], {
    "field",
    "int64_field",
    "double_field",
    "string_field"
  });
}

TEST_CASE( "add new columns with default values to record batch with different schema", "[DefaultHandler]" ) {
  auto schema = arrow::schema({arrow::field("field", arrow::null())});

  DefaultHandler::DefaultHandlerOptions options{
      {{"int64_field", {42}}},
      {{"double_field", {3.14}}},
      {{"string_field", {"Hello, world!"}}}
  };
  std::shared_ptr<RecordBatchHandler> default_handler = std::make_shared<DefaultHandler>(std::move(options));

  arrow::NullBuilder builder;
  arrowAssertNotOk(builder.AppendNull());
  std::shared_ptr<arrow::Array> array;
  arrowAssertNotOk(builder.Finish(&array));
  auto record_batch = arrow::RecordBatch::Make(schema, 1, {array});

  arrow::RecordBatchVector result;
  arrowAssertNotOk(default_handler->handle({record_batch}, &result));

  REQUIRE( result.size() == 1 );
  checkSize(result[0], 1, 4);
  checkColumnsArePresent(result[0], {
      "field",
      "int64_field",
      "double_field",
      "string_field"
  });
  checkValue<int64_t, arrow::Int64Scalar>(42, result[0],
                                          "int64_field", 0);
  checkValue<double, arrow::DoubleScalar>(3.14, result[0],
                                          "double_field", 0);
  checkValue<std::string, arrow::StringScalar>("Hello, world!", result[0],
                                          "string_field", 0);
}

TEST_CASE( "join on timestamp and tag column", "[JoinHandler]" ) {
  auto ts_field = arrow::field("time", arrow::timestamp(arrow::TimeUnit::SECOND));
  auto tag_field = arrow::field("tag", arrow::utf8());

  auto field_1_field = arrow::field("field_1", arrow::int64());
  auto schema_1 = arrow::schema({ts_field, tag_field, field_1_field});

  auto field_2_field = arrow::field("field_2", arrow::float64());
  auto schema_2 = arrow::schema({ts_field, tag_field, field_2_field});

  arrow::TimestampBuilder ts_builder(arrow::timestamp(arrow::TimeUnit::SECOND), arrow::default_memory_pool());
  arrowAssertNotOk(ts_builder.Append(100));
  std::shared_ptr<arrow::Array> ts_array_1;
  arrowAssertNotOk(ts_builder.Finish(&ts_array_1));

  arrowAssertNotOk(ts_builder.Append(100));
  std::shared_ptr<arrow::Array> ts_array_2;
  arrowAssertNotOk(ts_builder.Finish(&ts_array_2));

  arrow::StringBuilder tag_builder;
  arrowAssertNotOk(tag_builder.Append("tag_value"));
  std::shared_ptr<arrow::Array> tag_array_1;
  arrowAssertNotOk(tag_builder.Finish(&tag_array_1));

  arrowAssertNotOk(tag_builder.Append("tag_value"));
  std::shared_ptr<arrow::Array> tag_array_2;
  arrowAssertNotOk(tag_builder.Finish(&tag_array_2));

  arrow::Int64Builder field_1_builder;
  arrowAssertNotOk(field_1_builder.Append(42));
  std::shared_ptr<arrow::Array> field_1_array;
  arrowAssertNotOk(field_1_builder.Finish(&field_1_array));

  arrow::DoubleBuilder field_2_builder;
  arrowAssertNotOk(field_2_builder.Append(3.14));
  std::shared_ptr<arrow::Array> field_2_array;
  arrowAssertNotOk(field_2_builder.Finish(&field_2_array));

  arrow::RecordBatchVector record_batches;

  record_batches.push_back(arrow::RecordBatch::Make(schema_1, 1, {ts_array_1, tag_array_1, field_1_array}));
  arrowAssertNotOk(metadata::setTimeColumnNameMetadata(&record_batches.back(), ts_field->name()));
  record_batches.push_back(arrow::RecordBatch::Make(schema_2, 1, {ts_array_2, tag_array_2, field_2_array}));
  arrowAssertNotOk(metadata::setTimeColumnNameMetadata(&record_batches.back(), ts_field->name()));

  std::vector<std::string> join_on_columns{"tag"};
  std::shared_ptr<RecordBatchHandler> handler = std::make_shared<JoinHandler>(std::move(join_on_columns));

  arrow::RecordBatchVector result;
  arrowAssertNotOk(handler->handle(record_batches, &result));

  REQUIRE( result.size() == 1 );
  checkSize(result[0], 1, 4);
  checkColumnsArePresent(result[0], {
      "time", "tag", "field_1", "field_2"
  });
  checkValue<int64_t, arrow::Int64Scalar>(100, result[0],
                                          "time", 0);
  checkValue<std::string, arrow::StringScalar>("tag_value", result[0],
                                               "tag", 0);
  checkValue<int64_t, arrow::Int64Scalar>(42, result[0],
                                          "field_1", 0);
  checkValue<double, arrow::DoubleScalar>(3.14, result[0],
                                          "field_2", 0);
}

TEST_CASE( "assign missed values to null", "[JoinHandler]" ) {
  auto ts_field = arrow::field("time", arrow::timestamp(arrow::TimeUnit::SECOND));
  auto tag_field = arrow::field("tag", arrow::utf8());

  auto field_1_field = arrow::field("field_1", arrow::int64());
  auto schema_1 = arrow::schema({ts_field, tag_field, field_1_field});

  auto field_2_field = arrow::field("field_2", arrow::float64());
  auto schema_2 = arrow::schema({ts_field, tag_field, field_2_field});

  arrow::TimestampBuilder ts_builder(arrow::timestamp(arrow::TimeUnit::SECOND), arrow::default_memory_pool());
  arrowAssertNotOk(ts_builder.Append(105));
  arrowAssertNotOk(ts_builder.Append(110));
  std::shared_ptr<arrow::Array> ts_array_1;
  arrowAssertNotOk(ts_builder.Finish(&ts_array_1));

  arrowAssertNotOk(ts_builder.Append(110));
  std::shared_ptr<arrow::Array> ts_array_2;
  arrowAssertNotOk(ts_builder.Finish(&ts_array_2));

  arrow::StringBuilder tag_builder;
  arrowAssertNotOk(tag_builder.Append("tag_value"));
  arrowAssertNotOk(tag_builder.Append("other_tag_value"));
  std::shared_ptr<arrow::Array> tag_array_1;
  arrowAssertNotOk(tag_builder.Finish(&tag_array_1));

  arrowAssertNotOk(tag_builder.Append("tag_value"));
  std::shared_ptr<arrow::Array> tag_array_2;
  arrowAssertNotOk(tag_builder.Finish(&tag_array_2));

  arrow::Int64Builder field_1_builder;
  arrowAssertNotOk(field_1_builder.Append(43));
  arrowAssertNotOk(field_1_builder.Append(44));
  std::shared_ptr<arrow::Array> field_1_array;
  arrowAssertNotOk(field_1_builder.Finish(&field_1_array));

  arrow::DoubleBuilder field_2_builder;
  arrowAssertNotOk(field_2_builder.Append(2.71));
  std::shared_ptr<arrow::Array> field_2_array;
  arrowAssertNotOk(field_2_builder.Finish(&field_2_array));

  arrow::RecordBatchVector record_batches;

  record_batches.push_back(arrow::RecordBatch::Make(schema_1, 2, {ts_array_1, tag_array_1, field_1_array}));
  arrowAssertNotOk(metadata::setTimeColumnNameMetadata(&record_batches.back(), ts_field->name()));
  record_batches.push_back(arrow::RecordBatch::Make(schema_2, 1, {ts_array_2, tag_array_2, field_2_array}));
  arrowAssertNotOk(metadata::setTimeColumnNameMetadata(&record_batches.back(), ts_field->name()));

  std::vector<std::string> join_on_columns{"tag"};
  std::shared_ptr<RecordBatchHandler> handler = std::make_shared<JoinHandler>(std::move(join_on_columns));

  arrow::RecordBatchVector result;
  arrowAssertNotOk(handler->handle(record_batches, &result));

  REQUIRE( result.size() == 1 );
  checkSize(result[0], 3, 4);
  checkColumnsArePresent(result[0], {
      "time", "tag", "field_1", "field_2"
  });
  checkValue<int64_t, arrow::Int64Scalar>(105, result[0],
                                          "time", 0);
  checkValue<std::string, arrow::StringScalar>("tag_value", result[0],
                                               "tag", 0);
  checkValue<int64_t, arrow::Int64Scalar>(43, result[0],
                                          "field_1", 0);
  checkIsNull(result[0], "field_2", 0);

  checkValue<int64_t, arrow::Int64Scalar>(110, result[0],
                                          "time", 1);
  checkIsValid(result[0],"tag", 1);

  checkValue<int64_t, arrow::Int64Scalar>(110, result[0],
                                          "time", 2);
  checkIsValid(result[0],"tag", 2);
}

TEST_CASE( "join depending on tolerance", "[JoinHandler]" ) {
  auto ts_field = arrow::field("time", arrow::timestamp(arrow::TimeUnit::SECOND));
  auto tag_field = arrow::field("tag", arrow::utf8());

  auto field_1_field = arrow::field("field_1", arrow::int64());
  auto schema_1 = arrow::schema({ts_field, tag_field, field_1_field});

  auto field_2_field = arrow::field("field_2", arrow::float64());
  auto schema_2 = arrow::schema({ts_field, tag_field, field_2_field});

  arrow::TimestampBuilder ts_builder(arrow::timestamp(arrow::TimeUnit::SECOND), arrow::default_memory_pool());
  arrowAssertNotOk(ts_builder.Append(100));
  std::shared_ptr<arrow::Array> ts_array_1;
  arrowAssertNotOk(ts_builder.Finish(&ts_array_1));

  arrowAssertNotOk(ts_builder.Append(103));
  std::shared_ptr<arrow::Array> ts_array_2;
  arrowAssertNotOk(ts_builder.Finish(&ts_array_2));

  arrow::StringBuilder tag_builder;
  arrowAssertNotOk(tag_builder.Append("tag_value"));
  std::shared_ptr<arrow::Array> tag_array_1;
  arrowAssertNotOk(tag_builder.Finish(&tag_array_1));

  arrowAssertNotOk(tag_builder.Append("tag_value"));
  std::shared_ptr<arrow::Array> tag_array_2;
  arrowAssertNotOk(tag_builder.Finish(&tag_array_2));

  arrow::Int64Builder field_1_builder;
  arrowAssertNotOk(field_1_builder.Append(42));
  std::shared_ptr<arrow::Array> field_1_array;
  arrowAssertNotOk(field_1_builder.Finish(&field_1_array));

  arrow::DoubleBuilder field_2_builder;
  arrowAssertNotOk(field_2_builder.Append(3.14));
  std::shared_ptr<arrow::Array> field_2_array;
  arrowAssertNotOk(field_2_builder.Finish(&field_2_array));

  arrow::RecordBatchVector record_batches;

  record_batches.push_back(arrow::RecordBatch::Make(schema_1, 1, {ts_array_1, tag_array_1, field_1_array}));
  arrowAssertNotOk(metadata::setTimeColumnNameMetadata(&record_batches.back(), ts_field->name()));
  record_batches.push_back(arrow::RecordBatch::Make(schema_2, 1, {ts_array_2, tag_array_2, field_2_array}));
  arrowAssertNotOk(metadata::setTimeColumnNameMetadata(&record_batches.back(), ts_field->name()));

  std::vector<std::string> join_on_columns{"tag"};
  std::shared_ptr<RecordBatchHandler> handler = std::make_shared<JoinHandler>(std::move(join_on_columns), 5);

  arrow::RecordBatchVector result;
  arrowAssertNotOk(handler->handle(record_batches, &result));

  checkSize(result[0], 1, 4);
  checkColumnsArePresent(result[0], {
      "time", "tag", "field_1", "field_2"
  });
  checkIsValid(result[0],"time", 0);
  checkValue<std::string, arrow::StringScalar>("tag_value", result[0],
                                               "tag", 0);
  checkValue<int64_t, arrow::Int64Scalar>(42, result[0],
                                          "field_1", 0);
  checkValue<double, arrow::DoubleScalar>(3.14, result[0],
                                          "field_2", 0);
}

SCENARIO( "groups aggregation", "[AggregateHandler]" ) {
  GIVEN( "RecordBatches with different groups" ) {
    auto time_field =
        arrow::field("time", arrow::timestamp(arrow::TimeUnit::SECOND));
    auto tag_field = arrow::field("group_tag", arrow::utf8());
    auto schema = arrow::schema({time_field, tag_field});

    std::string group_1{"group_1"};
    std::string group_2{"group_2"};

    arrow::TimestampBuilder time_builder_0
        (arrow::timestamp(arrow::TimeUnit::SECOND),
         arrow::default_memory_pool());
    arrowAssertNotOk(time_builder_0.Append(100));
    std::shared_ptr<arrow::Array> time_array_0;
    arrowAssertNotOk(time_builder_0.Finish(&time_array_0));

    arrow::StringBuilder tag_builder_0;
    arrowAssertNotOk(tag_builder_0.Append(group_1));
    std::shared_ptr<arrow::Array> tag_array_0;
    arrowAssertNotOk(tag_builder_0.Finish(&tag_array_0));

    auto record_batch_0 =
        arrow::RecordBatch::Make(schema, 1, {time_array_0, tag_array_0});
    arrowAssertNotOk(metadata::fillGroupMetadata(&record_batch_0,
                                                 {tag_field->name()}));
    arrowAssertNotOk(metadata::setTimeColumnNameMetadata(&record_batch_0, time_field->name()));

    arrow::TimestampBuilder time_builder_1
        (arrow::timestamp(arrow::TimeUnit::SECOND),
         arrow::default_memory_pool());
    arrowAssertNotOk(time_builder_1.Append(101));
    std::shared_ptr<arrow::Array> time_array_1;
    arrowAssertNotOk(time_builder_1.Finish(&time_array_1));

    arrow::StringBuilder tag_builder_1;
    arrowAssertNotOk(tag_builder_1.Append(group_1));
    std::shared_ptr<arrow::Array> tag_array_1;
    arrowAssertNotOk(tag_builder_1.Finish(&tag_array_1));

    auto record_batch_1 =
        arrow::RecordBatch::Make(schema, 1, {time_array_1, tag_array_1});
    arrowAssertNotOk(metadata::fillGroupMetadata(&record_batch_1,
                                                 {tag_field->name()}));
    arrowAssertNotOk(metadata::setTimeColumnNameMetadata(&record_batch_1, time_field->name()));

    arrow::TimestampBuilder time_builder_2
        (arrow::timestamp(arrow::TimeUnit::SECOND),
         arrow::default_memory_pool());
    arrowAssertNotOk(time_builder_2.Append(102));
    std::shared_ptr<arrow::Array> time_array_2;
    arrowAssertNotOk(time_builder_2.Finish(&time_array_2));

    arrow::StringBuilder tag_builder_2;
    arrowAssertNotOk(tag_builder_2.Append(group_2));
    std::shared_ptr<arrow::Array> tag_array_2;
    arrowAssertNotOk(tag_builder_2.Finish(&tag_array_2));

    auto record_batch_2 =
        arrow::RecordBatch::Make(schema, 1, {time_array_2, tag_array_2});
    arrowAssertNotOk(metadata::fillGroupMetadata(&record_batch_2,
                                                 {tag_field->name()}));
    arrowAssertNotOk(metadata::setTimeColumnNameMetadata(&record_batch_2, time_field->name()));

    arrow::RecordBatchVector result;

    AggregateHandler::AggregateOptions options{
      {}, {AggregateHandler::AggregateFunctionEnumType::kLast, time_field->name()}
    };
    std::shared_ptr<RecordBatchHandler>
        handler = std::make_shared<AggregateHandler>(options);

    WHEN( "applies aggregation to RecordBatches of the same group" ) {
      arrow::RecordBatchVector record_batches{record_batch_0, record_batch_1};
      arrowAssertNotOk(handler->handle(record_batches, &result));

      THEN( "aggregation is applied to RecordBatches separately, but put result in the same RecordBatch" ) {
        REQUIRE( result.size() == 1 );
        checkSize(result[0], 2, 2);
        checkColumnsArePresent(result[0], {time_field->name(), tag_field->name()});
        checkValue<int64_t, arrow::Int64Scalar>(100, result[0], time_field->name(), 0);
        checkValue<int64_t, arrow::Int64Scalar>(101, result[0], time_field->name(), 1);
        checkValue<std::string, arrow::StringScalar>(group_1, result[0], tag_field->name(), 0);
        checkValue<std::string, arrow::StringScalar>(group_1, result[0], tag_field->name(), 1);
        REQUIRE( !metadata::extractGroupMetadata(result[0]).empty() );
      }
    }

    WHEN( "applies aggregation to RecordBatches of different groups" ) {
      arrow::RecordBatchVector record_batches{record_batch_0, record_batch_2};
      arrowAssertNotOk(handler->handle(record_batches, &result));

      THEN( "aggregation is applied to RecordBatches separately and put result in different RecordBatches" ) {
        REQUIRE( result.size() == 2 );

        if (!equals<std::string, arrow::StringScalar>(group_1, result[0], tag_field->name(), 0)) {
          std::swap(result[0], result[1]);
        }

        checkSize(result[0], 1, 2);
        checkColumnsArePresent(result[0], {time_field->name(), tag_field->name()});
        checkValue<int64_t, arrow::Int64Scalar>(100, result[0], time_field->name(), 0);
        checkValue<std::string, arrow::StringScalar>(group_1, result[0], tag_field->name(), 0);
        REQUIRE( !metadata::extractGroupMetadata(result[0]).empty() );

        checkSize(result[1], 1, 2);
        checkColumnsArePresent(result[1], {time_field->name(), tag_field->name()});
        checkValue<int64_t, arrow::Int64Scalar>(102, result[1], time_field->name(), 0);
        checkValue<std::string, arrow::StringScalar>(group_2, result[1], tag_field->name(), 0);
        REQUIRE( !metadata::extractGroupMetadata(result[1]).empty() );
      }
    }
  }
}

SCENARIO( "aggregating time", "[AggregateHandler]" ) {
  GIVEN( "RecordBatch with time column" ) {
    auto time_field =
        arrow::field("before_time",
                     arrow::timestamp(arrow::TimeUnit::SECOND));
    auto schema = arrow::schema({time_field});

    arrow::TimestampBuilder time_builder
        (arrow::timestamp(arrow::TimeUnit::SECOND),
         arrow::default_memory_pool());
    arrowAssertNotOk(time_builder.Append(100));
    std::shared_ptr<arrow::Array> time_array;
    arrowAssertNotOk(time_builder.Finish(&time_array));

    auto record_batch =
        arrow::RecordBatch::Make(schema, 1, {time_array});
    arrowAssertNotOk(metadata::setTimeColumnNameMetadata(&record_batch,
                                                             time_field->name()));

    AND_GIVEN("AggregateHandler with different result time column name") {

      std::string new_time_column_name = "after_time";

      AggregateHandler::AggregateOptions options{
          {},
          {AggregateHandler::AggregateFunctionEnumType::kLast, new_time_column_name}
      };
      std::shared_ptr<RecordBatchHandler>
          handler = std::make_shared<AggregateHandler>(options);

      arrow::RecordBatchVector result;

      WHEN("applies aggregation to RecordBatch") {
        arrow::RecordBatchVector record_batches{record_batch};
        arrowAssertNotOk(handler->handle(record_batches, &result));

        THEN("it changes time column name and corresponding time column name metadata") {
          REQUIRE(result.size() == 1);
          checkSize(result[0], 1, 1);
          checkColumnsArePresent(result[0],
                                 {new_time_column_name});
          checkValue<int64_t, arrow::Int64Scalar>(100,
                                                  result[0],
                                                  new_time_column_name,
                                                  0);
          std::string result_time_column_name;
          arrowAssertNotOk(metadata::getTimeColumnNameMetadata(result[0], &result_time_column_name));
          REQUIRE( result_time_column_name == new_time_column_name );
        }
      }
    }
  }
}
