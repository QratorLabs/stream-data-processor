#include <chrono>
#include <ctime>
#include <memory>
#include <vector>

#include <arrow/api.h>
#include <gandiva/tree_expr_builder.h>
#include <gmock/gmock.h>
#include <catch2/catch.hpp>

#include "metadata/column_typing.h"
#include "metadata/grouping.h"
#include "metadata/time_metadata.h"
#include "record_batch_handlers/record_batch_handlers.h"
#include "record_batch_handlers/stateful_handlers/threshold_state_machine.h"
#include "test_help.h"
#include "record_batch_builder.h"

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
  arrowAssignOrRaise(result, filter_handler->handle({record_batch}));

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
  arrowAssignOrRaise(result, filter_handler->handle({record_batch}));

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
  std::shared_ptr<RecordBatchHandler> group_handler = std::make_shared<GroupHandler>(std::move(grouping_columns));

  arrow::RecordBatchVector result;
  arrowAssignOrRaise(result, group_handler->handle({record_batch}));

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

  REQUIRE(metadata::extractGroupMetadata(*result[0]) !=
            metadata::extractGroupMetadata(*result[1]) );
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
  arrowAssignOrRaise(result, default_handler->handle({record_batch}));

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
  arrowAssignOrRaise(result, default_handler->handle({record_batch}));

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
  arrowAssignOrRaise(result, handler->handle(record_batches));

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
  arrowAssignOrRaise(result, handler->handle(record_batches));

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
  arrowAssignOrRaise(result, handler->handle(record_batches));

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
      arrowAssignOrRaise(result, handler->handle(record_batches));

      THEN( "aggregation is applied to RecordBatches separately, but put result in the same RecordBatch" ) {
        REQUIRE( result.size() == 1 );
        checkSize(result[0], 2, 2);
        checkColumnsArePresent(result[0], {time_field->name(), tag_field->name()});
        checkValue<int64_t, arrow::Int64Scalar>(100, result[0], time_field->name(), 0);
        checkValue<int64_t, arrow::Int64Scalar>(101, result[0], time_field->name(), 1);
        checkValue<std::string, arrow::StringScalar>(group_1, result[0], tag_field->name(), 0);
        checkValue<std::string, arrow::StringScalar>(group_1, result[0], tag_field->name(), 1);
        REQUIRE( !metadata::extractGroupMetadata(*result[0]).empty() );
      }
    }

    WHEN( "applies aggregation to RecordBatches of different groups" ) {
      arrow::RecordBatchVector record_batches{record_batch_0, record_batch_2};
      arrowAssignOrRaise(result, handler->handle(record_batches));

      THEN( "aggregation is applied to RecordBatches separately and put result in different RecordBatches" ) {
        REQUIRE( result.size() == 2 );

        if (!equals<std::string, arrow::StringScalar>(group_1, result[0], tag_field->name(), 0)) {
          std::swap(result[0], result[1]);
        }

        checkSize(result[0], 1, 2);
        checkColumnsArePresent(result[0], {time_field->name(), tag_field->name()});
        checkValue<int64_t, arrow::Int64Scalar>(100, result[0], time_field->name(), 0);
        checkValue<std::string, arrow::StringScalar>(group_1, result[0], tag_field->name(), 0);
        REQUIRE( !metadata::extractGroupMetadata(*result[0]).empty() );

        checkSize(result[1], 1, 2);
        checkColumnsArePresent(result[1], {time_field->name(), tag_field->name()});
        checkValue<int64_t, arrow::Int64Scalar>(102, result[1], time_field->name(), 0);
        checkValue<std::string, arrow::StringScalar>(group_2, result[1], tag_field->name(), 0);
        REQUIRE( !metadata::extractGroupMetadata(*result[1]).empty() );
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
        arrowAssignOrRaise(result, handler->handle(record_batches));

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
          arrowAssignOrRaise(result_time_column_name, metadata::getTimeColumnNameMetadata(*result[0]));
          REQUIRE( result_time_column_name == new_time_column_name );
        }
      }
    }
  }
}

using namespace std::chrono_literals;

SCENARIO( "threshold state machine changes states", "[ThresholdStateMachine]" ) {
  GIVEN("ThresholdStateMachine with initial state") {
    ThresholdStateMachine::Options options{
        "value", "level",
        10,
        2, 5s,
        0.3, 0.5, 5s
    };

    std::shared_ptr<HandlerFactory> factory =
        std::make_shared<ThresholdStateMachineFactory>(options);

    std::shared_ptr<ThresholdStateMachine> state_machine =
        std::static_pointer_cast<ThresholdStateMachine>(factory->createHandler());

    REQUIRE(instanceOf<internal::StateOK>(state_machine->getState().get()));

    RecordBatchBuilder builder;
    builder.reset();

    std::string time_column_name{"time"};

    std::shared_ptr<arrow::RecordBatch> record_batch;
    arrow::RecordBatchVector result;

    AND_GIVEN("RecordBatch with ok value") {
      auto now = std::time(nullptr);
      int64_t value = 7;
      arrowAssertNotOk(builder.setRowNumber(1));

      arrowAssertNotOk(builder.buildTimeColumn<std::time_t>(
          time_column_name, {now}, arrow::TimeUnit::SECOND));

      arrowAssertNotOk(builder.buildColumn<int64_t>(options.watch_column_name,
                                                    {value}));
      arrowAssignOrRaise(record_batch, builder.getResult());

      WHEN("ThresholdStateMachine is applied to the RecordBatch") {
        arrowAssignOrRaise(result, state_machine->handle(record_batch));

        THEN("threshold column is added and state is OK") {
          REQUIRE(result.size() == 1);

          checkSize(result[0], 1, 3);

          checkColumnsArePresent(result[0], {
              time_column_name, options.watch_column_name,
              options.threshold_column_name
          });

          checkValue<int64_t, arrow::Int64Scalar>(
              now, result[0], time_column_name, 0);
          checkValue<int64_t, arrow::Int64Scalar>(
              value, result[0], options.watch_column_name, 0);
          checkValue<double, arrow::DoubleScalar>(
              options.default_threshold, result[0], options.threshold_column_name, 0);

          REQUIRE(instanceOf<internal::StateOK>(state_machine->getState().get()));
        }
      }
    }

    AND_GIVEN("RecordBatch with value bigger than threshold") {
      auto now = std::time(nullptr);
      int64_t value = 15;
      arrowAssertNotOk(builder.setRowNumber(1));

      arrowAssertNotOk(builder.buildTimeColumn<std::time_t>(
          time_column_name, {now}, arrow::TimeUnit::SECOND));

      arrowAssertNotOk(builder.buildColumn<int64_t>(options.watch_column_name,
                                                    {value}));
      arrowAssignOrRaise(record_batch, builder.getResult());

      WHEN("ThresholdStateMachine is applied to the RecordBatch") {
        arrowAssignOrRaise(result, state_machine->handle(record_batch));

        THEN("threshold column is added and state is Alert") {
          REQUIRE(result.size() == 1);

          checkSize(result[0], 1, 3);

          checkColumnsArePresent(result[0], {
              time_column_name, options.watch_column_name,
              options.threshold_column_name
          });

          checkValue<int64_t, arrow::Int64Scalar>(
              now, result[0], time_column_name, 0);
          checkValue<int64_t, arrow::Int64Scalar>(
              value, result[0], options.watch_column_name, 0);
          checkValue<double, arrow::DoubleScalar>(
              options.default_threshold, result[0], options.threshold_column_name, 0);

          REQUIRE(instanceOf<internal::StateIncrease>(state_machine->getState().get()));

          AND_WHEN("value keeps bigger than threshold exceeding alert duration") {
            auto next_time = now + options.increase_after.count() + 1;
            value = 17;
            builder.reset();
            arrowAssertNotOk(builder.setRowNumber(1));

            arrowAssertNotOk(builder.buildTimeColumn<std::time_t>(
                time_column_name, {next_time}, arrow::TimeUnit::SECOND));

            arrowAssertNotOk(builder.buildColumn<int64_t>(options.watch_column_name,
                                                          {value}));
            arrowAssignOrRaise(record_batch, builder.getResult());

            result.clear();
            arrowAssignOrRaise(result, state_machine->handle(record_batch));

            THEN("increased threshold column is added and state is OK") {
              REQUIRE(result.size() == 1);

              checkSize(result[0], 1, 3);

              checkColumnsArePresent(result[0], {
                  time_column_name, options.watch_column_name,
                  options.threshold_column_name
              });

              checkValue<int64_t, arrow::Int64Scalar>(
                  next_time, result[0], time_column_name, 0);
              checkValue<int64_t, arrow::Int64Scalar>(
                  value, result[0], options.watch_column_name, 0);
              checkValue<double, arrow::DoubleScalar>(
                  options.default_threshold * options.increase_scale_factor,
                  result[0],
                  options.threshold_column_name,
                  0);

              REQUIRE(instanceOf<internal::StateOK>(state_machine->getState().get()));
            }
          }

          AND_WHEN("value is flapping near the threshold") {
            auto next_time = now + 2;
            value = 9;
            builder.reset();
            arrowAssertNotOk(builder.setRowNumber(1));

            arrowAssertNotOk(builder.buildTimeColumn<std::time_t>(
                time_column_name, {next_time}, arrow::TimeUnit::SECOND));

            arrowAssertNotOk(builder.buildColumn<int64_t>(options.watch_column_name,
                                                          {value}));
            arrowAssignOrRaise(record_batch, builder.getResult());

            result.clear();
            arrowAssignOrRaise(result, state_machine->handle(record_batch));

            THEN("threshold column is added and state is OK") {
              REQUIRE(result.size() == 1);

              checkSize(result[0], 1, 3);

              checkColumnsArePresent(result[0], {
                  time_column_name, options.watch_column_name,
                  options.threshold_column_name
              });

              checkValue<int64_t, arrow::Int64Scalar>(
                  next_time, result[0], time_column_name, 0);
              checkValue<int64_t, arrow::Int64Scalar>(
                  value, result[0], options.watch_column_name, 0);
              checkValue<double, arrow::DoubleScalar>(
                  options.default_threshold,
                  result[0],
                  options.threshold_column_name,
                  0);

              REQUIRE(instanceOf<internal::StateOK>(state_machine->getState().get()));
            }
          }
        }
      }
    }

    AND_GIVEN("RecordBatch with value lower than threshold times decrease_trigger_factor") {
      auto now = std::time(nullptr);
      double value = options.default_threshold * options.decrease_trigger_factor - 0.1;
      arrowAssertNotOk(builder.setRowNumber(1));

      arrowAssertNotOk(builder.buildTimeColumn<std::time_t>(
          time_column_name, {now}, arrow::TimeUnit::SECOND));

      arrowAssertNotOk(builder.buildColumn<double>(options.watch_column_name,
                                                    {value}));
      arrowAssignOrRaise(record_batch, builder.getResult());

      WHEN("ThresholdStateMachine is applied to the RecordBatch") {
        arrowAssignOrRaise(result, state_machine->handle(record_batch));

        THEN("threshold column is added and state is Decrease") {
          REQUIRE(result.size() == 1);

          checkSize(result[0], 1, 3);

          checkColumnsArePresent(result[0], {
              time_column_name, options.watch_column_name,
              options.threshold_column_name
          });

          checkValue<int64_t, arrow::Int64Scalar>(
              now, result[0], time_column_name, 0);
          checkValue<double, arrow::DoubleScalar>(
              value, result[0], options.watch_column_name, 0);
          checkValue<double, arrow::DoubleScalar>(
              options.default_threshold,
              result[0],
              options.threshold_column_name,
              0);

          REQUIRE(instanceOf<internal::StateDecrease>(state_machine->getState().get()));

          AND_WHEN("value keeps low exceeding descrease timeout") {
            auto next_time = now + options.decrease_after.count() + 1;
            value -= 0.1;
            builder.reset();
            arrowAssertNotOk(builder.setRowNumber(1));

            arrowAssertNotOk(builder.buildTimeColumn<std::time_t>(
                time_column_name, {next_time}, arrow::TimeUnit::SECOND));

            arrowAssertNotOk(builder.buildColumn<double>(options.watch_column_name,
                                                         {value}));
            arrowAssignOrRaise(record_batch, builder.getResult());

            result.clear();
            arrowAssignOrRaise(result, state_machine->handle(record_batch));

            THEN("decreased threshold column is added and state returns to OK") {
              REQUIRE(result.size() == 1);

              checkSize(result[0], 1, 3);

              checkColumnsArePresent(result[0], {
                  time_column_name, options.watch_column_name,
                  options.threshold_column_name
              });

              checkValue<int64_t, arrow::Int64Scalar>(
                  next_time, result[0], time_column_name, 0);
              checkValue<double, arrow::DoubleScalar>(
                  value, result[0], options.watch_column_name, 0);
              checkValue<double, arrow::DoubleScalar>(
                  options.default_threshold * options.decrease_scale_factor,
                  result[0],
                  options.threshold_column_name,
                  0);

              REQUIRE(instanceOf<internal::StateOK>(state_machine->getState().get()));
            }
          }

          AND_WHEN("value is flapping near the decrease trigger level") {
            auto next_time = now + 2;
            value += 0.2;
            builder.reset();
            arrowAssertNotOk(builder.setRowNumber(1));

            arrowAssertNotOk(builder.buildTimeColumn<std::time_t>(
                time_column_name, {next_time}, arrow::TimeUnit::SECOND));

            arrowAssertNotOk(builder.buildColumn<double>(options.watch_column_name,
                                                          {value}));
            arrowAssignOrRaise(record_batch, builder.getResult());

            result.clear();
            arrowAssignOrRaise(result, state_machine->handle(record_batch));

            THEN("threshold column is added and state returns to OK") {
              REQUIRE(result.size() == 1);

              checkSize(result[0], 1, 3);

              checkColumnsArePresent(result[0], {
                  time_column_name, options.watch_column_name,
                  options.threshold_column_name
              });

              checkValue<int64_t, arrow::Int64Scalar>(
                  next_time, result[0], time_column_name, 0);
              checkValue<double, arrow::DoubleScalar>(
                  value, result[0], options.watch_column_name, 0);
              checkValue<double, arrow::DoubleScalar>(
                  options.default_threshold,
                  result[0],
                  options.threshold_column_name,
                  0);

              REQUIRE(instanceOf<internal::StateOK>(state_machine->getState().get()));
            }
          }
        }
      }
    }
  }
}

TEST_CASE( "threshold not increasing over max", "[ThresholdStateMachine]" ) {
  ThresholdStateMachine::Options options{
    "value", "level",
    20,
    2, 1s
  };
  options.max_threshold = 30;

  std::shared_ptr<HandlerFactory> factory =
      std::make_shared<ThresholdStateMachineFactory>(options);

  std::shared_ptr<ThresholdStateMachine> state_machine =
      std::static_pointer_cast<ThresholdStateMachine>(factory->createHandler());

  RecordBatchBuilder builder;
  builder.reset();
  arrowAssertNotOk(builder.setRowNumber(2));
  std::string time_column_name{"time"};

  arrowAssertNotOk(builder.buildTimeColumn<std::time_t>(
      time_column_name, {100, 102}, arrow::TimeUnit::SECOND));

  arrowAssertNotOk(builder.buildColumn<double>(
      options.watch_column_name, {40, 50}));

  std::shared_ptr<arrow::RecordBatch> record_batch;
  arrowAssignOrRaise(record_batch, builder.getResult());

  arrow::RecordBatchVector result;
  arrowAssignOrRaise(result, state_machine->handle(record_batch));

  REQUIRE( result.size() == 1 );
  checkSize(result[0], 2, 3);
  checkColumnsArePresent(result[0], {
      time_column_name, options.watch_column_name,
      options.threshold_column_name
  });

  checkValue<int64_t, arrow::Int64Scalar>(
      100, result[0], time_column_name, 0);
  checkValue<double, arrow::DoubleScalar>(
      40, result[0], options.watch_column_name, 0);
  checkValue<double, arrow::DoubleScalar>(
      options.default_threshold,
      result[0],
      options.threshold_column_name,
      0);

  checkValue<int64_t, arrow::Int64Scalar>(
      102, result[0], time_column_name, 1);
  checkValue<double, arrow::DoubleScalar>(
      50, result[0], options.watch_column_name, 1);
  checkValue<double, arrow::DoubleScalar>(
      options.max_threshold,
      result[0],
      options.threshold_column_name,
      1);
}

TEST_CASE( "threshold not decreasing over min", "[ThresholdStateMachine]" ) {
  ThresholdStateMachine::Options options{
      "value", "level",
      20,
      2, 1s,
      0.4, 0.5, 1s
  };
  options.min_threshold = 15;

  std::shared_ptr<HandlerFactory> factory =
      std::make_shared<ThresholdStateMachineFactory>(options);

  std::shared_ptr<ThresholdStateMachine> state_machine =
      std::static_pointer_cast<ThresholdStateMachine>(factory->createHandler());

  RecordBatchBuilder builder;
  builder.reset();
  arrowAssertNotOk(builder.setRowNumber(2));
  std::string time_column_name{"time"};

  arrowAssertNotOk(builder.buildTimeColumn<std::time_t>(
      time_column_name, {100, 102}, arrow::TimeUnit::SECOND));

  arrowAssertNotOk(builder.buildColumn<double>(
      options.watch_column_name, {5, 3}));

  std::shared_ptr<arrow::RecordBatch> record_batch;
  arrowAssignOrRaise(record_batch, builder.getResult());

  arrow::RecordBatchVector result;
  arrowAssignOrRaise(result, state_machine->handle(record_batch));

  REQUIRE( result.size() == 1 );
  checkSize(result[0], 2, 3);
  checkColumnsArePresent(result[0], {
      time_column_name, options.watch_column_name,
      options.threshold_column_name
  });

  checkValue<int64_t, arrow::Int64Scalar>(
      100, result[0], time_column_name, 0);
  checkValue<double, arrow::DoubleScalar>(
      5, result[0], options.watch_column_name, 0);
  checkValue<double, arrow::DoubleScalar>(
      options.default_threshold,
      result[0],
      options.threshold_column_name,
      0);

  checkValue<int64_t, arrow::Int64Scalar>(
      102, result[0], time_column_name, 1);
  checkValue<double, arrow::DoubleScalar>(
      3, result[0], options.watch_column_name, 1);
  checkValue<double, arrow::DoubleScalar>(
      options.min_threshold,
      result[0],
      options.threshold_column_name,
      1);
}

SCENARIO( "WindowHandler behaviour with true fill_period flag", "[WindowHandler]" ) {
  GIVEN( "WindowHandler with fill_period flag set to true" ) {
    WindowHandler::WindowOptions options{5s, 3s, true};

    std::unique_ptr<RecordBatchHandler> handler =
        std::make_unique<WindowHandler>(std::move(options));

    RecordBatchBuilder builder;
    std::string time_column_name{"time"};
    std::shared_ptr<arrow::RecordBatch> record_batch;
    arrow::RecordBatchVector result;
    WHEN( "pass batch with part of window" ) {
      builder.reset();
      arrowAssertNotOk(builder.setRowNumber(2));

      arrowAssertNotOk(builder.buildTimeColumn<std::time_t>(
          time_column_name, {0, 4}, arrow::TimeUnit::SECOND));

      arrowAssignOrRaise(record_batch, builder.getResult());
      arrowAssignOrRaise(result, handler->handle(record_batch));

      THEN( "handler returns empty result" ) {
        REQUIRE(result.size() == 0);

        AND_WHEN( "pass batch with bigger timestamps" ) {
          builder.reset();
          arrowAssertNotOk(builder.setRowNumber(2));

          arrowAssertNotOk(builder.buildTimeColumn<std::time_t>(
              time_column_name, {5, 6}, arrow::TimeUnit::SECOND));

          arrowAssignOrRaise(record_batch, builder.getResult());
          arrowAssignOrRaise(result, handler->handle(record_batch));

          THEN( "window is emitted" ) {
            REQUIRE(result.size() == 1);
            checkSize(result[0], 2, 1);
            checkColumnsArePresent(result[0], {time_column_name});

            checkValue<int64_t, arrow::Int64Scalar>(
                0, result[0], time_column_name, 0);
            checkValue<int64_t, arrow::Int64Scalar>(
                4, result[0], time_column_name, 1);
          }
        }
      }
    }

    WHEN( "record batch contains several windows" ) {
      builder.reset();
      arrowAssertNotOk(builder.setRowNumber(4));

      arrowAssertNotOk(builder.buildTimeColumn<std::time_t>(
          time_column_name, {0, 4, 6, 8}, arrow::TimeUnit::SECOND));

      arrowAssignOrRaise(record_batch, builder.getResult());
      arrowAssignOrRaise(result, handler->handle(record_batch));

      THEN( "all of them are emitted" ) {
        REQUIRE( result.size() == 2 );

        checkSize(result[0], 2, 1);
        checkColumnsArePresent(result[0], {time_column_name});
        checkValue<int64_t, arrow::Int64Scalar>(
            0, result[0], time_column_name, 0);
        checkValue<int64_t, arrow::Int64Scalar>(
            4, result[0], time_column_name, 1);

        checkSize(result[1], 2, 1);
        checkColumnsArePresent(result[1], {time_column_name});
        checkValue<int64_t, arrow::Int64Scalar>(
            4, result[1], time_column_name, 0);
        checkValue<int64_t, arrow::Int64Scalar>(
            6, result[1], time_column_name, 1);
      }
    }
  }
}

SCENARIO( "WindowHandler behaviour with false fill_period flag", "[WindowHandler]" ) {
  GIVEN( "WindowHandler with fill_period flag set to false" ) {
    WindowHandler::WindowOptions options{5s, 3s, false};

    std::unique_ptr<RecordBatchHandler> handler =
        std::make_unique<WindowHandler>(std::move(options));

    RecordBatchBuilder builder;
    std::string time_column_name{"time"};
    std::shared_ptr<arrow::RecordBatch> record_batch;
    arrow::RecordBatchVector result;
    WHEN( "pass batch with part of window" ) {
      builder.reset();
      arrowAssertNotOk(builder.setRowNumber(2));

      arrowAssertNotOk(builder.buildTimeColumn<std::time_t>(
          time_column_name, {0, 4}, arrow::TimeUnit::SECOND));

      arrowAssignOrRaise(record_batch, builder.getResult());
      arrowAssignOrRaise(result, handler->handle(record_batch));

      THEN( "handler emits truncated window" ) {
        REQUIRE(result.size() == 1);

        checkSize(result[0], 1, 1);
        checkColumnsArePresent(result[0], {time_column_name});

        checkValue<int64_t, arrow::Int64Scalar>(
            0, result[0], time_column_name, 0);

        AND_WHEN( "pass batch with bigger timestamps" ) {
          builder.reset();
          arrowAssertNotOk(builder.setRowNumber(2));

          arrowAssertNotOk(builder.buildTimeColumn<std::time_t>(
              time_column_name, {5, 6}, arrow::TimeUnit::SECOND));

          arrowAssignOrRaise(record_batch, builder.getResult());
          arrowAssignOrRaise(result, handler->handle(record_batch));

          THEN( "another window is emitted" ) {
            REQUIRE(result.size() == 1);
            checkSize(result[0], 2, 1);
            checkColumnsArePresent(result[0], {time_column_name});

            checkValue<int64_t, arrow::Int64Scalar>(
                4, result[0], time_column_name, 0);
            checkValue<int64_t, arrow::Int64Scalar>(
                5, result[0], time_column_name, 1);
          }
        }
      }
    }
  }
}

SCENARIO( "GroupHandler preserves old group metadata", "[GroupHandler]") {
  GIVEN( "RecordBatch with two columns" ) {
    RecordBatchBuilder builder;
    builder.reset();
    arrowAssertNotOk(builder.setRowNumber(1));

    std::string column_name_1{"column_1"};
    std::string column_name_2{"column_2"};
    arrowAssertNotOk(builder.buildColumn<int64_t>(column_name_1, {1}));
    arrowAssertNotOk(builder.buildColumn<int64_t>(column_name_2, {2}));
    std::shared_ptr<arrow::RecordBatch> record_batch;
    arrowAssignOrRaise(record_batch, builder.getResult());

    WHEN ( "groups by first column" ) {
      std::vector<std::string> grouping_columns_1{column_name_1};
      std::shared_ptr<RecordBatchHandler> group_handler =
          std::make_shared<GroupHandler>(std::move(grouping_columns_1));

      arrow::RecordBatchVector result;
      arrowAssignOrRaise(result, group_handler->handle(record_batch));

      THEN ( "group metadata is set" ) {
        REQUIRE(result.size() == 1);
        checkSize(result[0], 1, 2);
        checkColumnsArePresent(result[0], {column_name_1, column_name_2});
        checkValue<int64_t, arrow::Int64Scalar>(1, result[0],
                                                column_name_1, 0);
        checkValue<int64_t, arrow::Int64Scalar>(2, result[0],
                                                column_name_2, 0);

        auto group_column_names =
            metadata::extractGroupingColumnsNames(*result[0]);
        REQUIRE(group_column_names.size() == 1);
        REQUIRE(group_column_names[0] == column_name_1);

        AND_WHEN( "group by second column" ) {
          std::vector<std::string> grouping_columns_2{column_name_2};
          group_handler =
              std::make_shared<GroupHandler>(std::move(grouping_columns_2));

          arrowAssignOrRaise(result, group_handler->handle(result[0]));

          THEN ( "group metadata from first grouping is preserved and the metadata from second grouping is added" ) {
            REQUIRE(result.size() == 1);
            checkSize(result[0], 1, 2);
            checkColumnsArePresent(result[0], {column_name_1, column_name_2});
            checkValue<int64_t, arrow::Int64Scalar>(1, result[0],
                                                    column_name_1, 0);
            checkValue<int64_t, arrow::Int64Scalar>(2, result[0],
                                                    column_name_2, 0);

            group_column_names =
                metadata::extractGroupingColumnsNames(*result[0]);
            REQUIRE(group_column_names.size() == 2);
            if (group_column_names[0] != column_name_1) {
              std::swap(group_column_names[0], group_column_names[1]);
            }

            REQUIRE(group_column_names[0] == column_name_1);
            REQUIRE(group_column_names[1] == column_name_2);
          }
        }
      }
    }
  }
}

TEST_CASE( "aggregating grouped by time column", "[AggregateHandler]" ) {
  RecordBatchBuilder builder;
  builder.reset();
  arrowAssertNotOk(builder.setRowNumber(1));

  std::string time_column_name{"time"};
  std::vector<std::time_t> time_values{100};
  arrowAssertNotOk(builder.buildTimeColumn(time_column_name, time_values, arrow::TimeUnit::SECOND));

  std::shared_ptr<arrow::RecordBatch> record_batch;
  arrowAssignOrRaise(record_batch, builder.getResult());
  arrowAssertNotOk(metadata::fillGroupMetadata(&record_batch, {time_column_name}));

  AggregateHandler::AggregateOptions options;
  std::unique_ptr<RecordBatchHandler> handler = std::make_unique<AggregateHandler>(options);

  arrow::RecordBatchVector result;
  arrowAssignOrRaise(result, handler->handle(record_batch));

  REQUIRE( result.size() == 1 );
  checkSize(result[0], 1, 1);
  checkColumnsArePresent(result[0], {options.result_time_column_rule.result_column_name});
  checkValue<int64_t, arrow::Int64Scalar>(
      100, result[0],
      options.result_time_column_rule.result_column_name, 0);
}

SCENARIO( "WindowHandler behaviour with empty window", "[WindowHandler]" ) {
  GIVEN( "WindowHandler with fill_period flag set to false" ) {
    WindowHandler::WindowOptions options{5s, 3s, true};

    std::unique_ptr<RecordBatchHandler> handler =
        std::make_unique<WindowHandler>(std::move(options));

    RecordBatchBuilder builder;
    std::string time_column_name{"time"};
    std::shared_ptr<arrow::RecordBatch> record_batch;
    arrow::RecordBatchVector result;
    WHEN( "pass batch with empty window inside" ) {
      builder.reset();
      arrowAssertNotOk(builder.setRowNumber(3));

      arrowAssertNotOk(builder.buildTimeColumn<std::time_t>(
          time_column_name, {0, 10, 11}, arrow::TimeUnit::SECOND));

      arrowAssignOrRaise(record_batch, builder.getResult());
      arrowAssignOrRaise(result, handler->handle(record_batch));

      THEN( "handler emits both windows" ) {
        REQUIRE(result.size() == 2);

        checkSize(result[0], 1, 1);
        checkColumnsArePresent(result[0], {time_column_name});

        checkValue<int64_t, arrow::Int64Scalar>(
            0, result[0], time_column_name, 0);

        checkSize(result[1], 1, 1);
        checkColumnsArePresent(result[1], {time_column_name});

        checkValue<int64_t, arrow::Int64Scalar>(
            10, result[1], time_column_name, 0);
      }
    }
  }
}

SCENARIO( "WindowHandler correctly handles timestamps with arrow time type different from SECONDS", "[WindowHandler]" ) {
  GIVEN( "WindowHandler with fill_period flag set to false" ) {
    WindowHandler::WindowOptions options{5s, 3s, false};

    std::unique_ptr<RecordBatchHandler> handler =
        std::make_unique<WindowHandler>(std::move(options));

    RecordBatchBuilder builder;
    std::string time_column_name{"time"};
    std::shared_ptr<arrow::RecordBatch> record_batch;
    arrow::RecordBatchVector result;
    WHEN( "pass batch with part of window" ) {
      builder.reset();
      arrowAssertNotOk(builder.setRowNumber(2));

      arrowAssertNotOk(builder.buildTimeColumn<int64_t>(
          time_column_name, {0, 4000000000}, arrow::TimeUnit::NANO));

      arrowAssignOrRaise(record_batch, builder.getResult());
      arrowAssignOrRaise(result, handler->handle(record_batch));

      THEN( "handler emits truncated window" ) {
        REQUIRE(result.size() == 1);

        checkSize(result[0], 1, 1);
        checkColumnsArePresent(result[0], {time_column_name});

        checkValue<int64_t, arrow::Int64Scalar>(
            0, result[0], time_column_name, 0);

        AND_WHEN( "pass batch with bigger timestamps" ) {
          builder.reset();
          arrowAssertNotOk(builder.setRowNumber(2));

          arrowAssertNotOk(builder.buildTimeColumn<std::time_t>(
              time_column_name, {5000000000, 6000000000}, arrow::TimeUnit::NANO));

          arrowAssignOrRaise(record_batch, builder.getResult());
          arrowAssignOrRaise(result, handler->handle(record_batch));

          THEN( "another window is emitted" ) {
            REQUIRE(result.size() == 1);
            checkSize(result[0], 2, 1);
            checkColumnsArePresent(result[0], {time_column_name});

            checkValue<int64_t, arrow::Int64Scalar>(
                4000000000, result[0], time_column_name, 0);
            checkValue<int64_t, arrow::Int64Scalar>(
                5000000000, result[0], time_column_name, 1);
          }
        }
      }
    }
  }
}

class MockWindowHandler : public IWindowHandler {
 public:
  MOCK_METHOD(arrow::Result<arrow::RecordBatchVector>, handle, (const std::shared_ptr<arrow::RecordBatch>& record_batch), (override));
  MOCK_METHOD(std::chrono::seconds, getEveryOption, (), (const, override));
  MOCK_METHOD(std::chrono::seconds, getPeriodOption, (), (const, override));
  MOCK_METHOD(void, setEveryOption, (const std::chrono::seconds& new_every_option, std::time_t change_ts), (override));
  MOCK_METHOD(void, setPeriodOption, (const std::chrono::seconds& new_period_option, std::time_t change_ts), (override));
};

SCENARIO("DynamicWindowHandler tries to change WindowHandler options when new options are arrived",
         "[DynamicWindowHandler]") {
  GIVEN("WindowHandler injected into DynamicWindowHandler and RecordBatch with new options") {
    using namespace ::testing;

    std::shared_ptr<MockWindowHandler> mock_window_handler =
        std::make_shared<NiceMock<MockWindowHandler>>();

    DynamicWindowHandler::DynamicWindowOptions dynamic_window_options{
        "every", "period"
    };

    std::unique_ptr<RecordBatchHandler> handler =
        std::make_unique<DynamicWindowHandler>(
            dynamic_window_options, mock_window_handler
        );

    int64_t new_every_option = 10;
    int64_t new_period_option = 15;

    RecordBatchBuilder builder;
    builder.reset();
    arrowAssertNotOk(builder.setRowNumber(1));
    arrowAssertNotOk(builder.buildTimeColumn<int64_t>("time",
                                                      {100},
                                                      arrow::TimeUnit::SECOND));
    arrowAssertNotOk(builder.buildColumn<int64_t>(
        dynamic_window_options.every_column_name, {new_every_option}));
    arrowAssertNotOk(builder.buildColumn<int64_t>(
        dynamic_window_options.period_column_name, {new_period_option}));
    std::shared_ptr<arrow::RecordBatch> record_batch;
    arrowAssignOrRaise(record_batch, builder.getResult());
    arrowAssertNotOk(metadata::setTimeUnitMetadata(
        &record_batch,
        dynamic_window_options.every_column_name,
        time_utils::SECOND));
    arrowAssertNotOk(metadata::setTimeUnitMetadata(
        &record_batch,
        dynamic_window_options.period_column_name,
        time_utils::SECOND));

    WHEN("RecordBatch with new options is passed to handler") {
      std::chrono::seconds current_every_duration{5};
      std::chrono::seconds new_every_duration{new_every_option};

      std::chrono::seconds current_period_duration{10};
      std::chrono::seconds new_period_duration{new_period_option};
      THEN("DynamicWindowHandler is trying to change WindowHandler options") {
        ON_CALL(*mock_window_handler,
                    getEveryOption())
            .WillByDefault([&]() { return current_every_duration; });

        ON_CALL(*mock_window_handler,
                    setEveryOption(Eq(new_every_duration), _))
            .WillByDefault([&]() { current_every_duration = new_every_duration; });

        EXPECT_CALL(*mock_window_handler,
                    getEveryOption())
            .Times(AtLeast(1));

        EXPECT_CALL(*mock_window_handler,
                    setEveryOption(Eq(new_every_duration), _))
            .Times(AtLeast(1));


        ON_CALL(*mock_window_handler,
                    getPeriodOption())
            .WillByDefault([&]() { return current_period_duration; });

        ON_CALL(*mock_window_handler,
                    setPeriodOption(Eq(new_period_duration), _))
            .WillByDefault([&]() { current_period_duration = new_period_duration; });

        EXPECT_CALL(*mock_window_handler,
                    getPeriodOption())
            .Times(AtLeast(1));

        EXPECT_CALL(*mock_window_handler,
                    setPeriodOption(Eq(new_period_duration), _))
            .Times(AtLeast(1));


        EXPECT_CALL(*mock_window_handler,
                    handle(_))
            .WillOnce(Return(arrow::RecordBatchVector{}));
      }

      auto result = handler->handle(record_batch);
      arrowAssertNotOk(result.status());
    }
  }
}

TEST_CASE("WindowHandler preserves metadata", "[WindowHandler]") {
  WindowHandler::WindowOptions options{5s, 3s, true};

  std::unique_ptr<RecordBatchHandler> handler =
      std::make_unique<WindowHandler>(std::move(options));

  RecordBatchBuilder builder;
  builder.reset();
  arrowAssertNotOk(builder.setRowNumber(2));

  std::string time_column_name{"time"};
  arrowAssertNotOk(builder.buildTimeColumn<std::time_t>(
      time_column_name, {0, 10}, arrow::TimeUnit::SECOND));

  std::string tag_column_name{"tag"};
  arrowAssertNotOk(builder.buildColumn<std::string>(
      tag_column_name, {"tag_value", "tag_value"}));

  std::string duration_column_name{"duration"};
  arrowAssertNotOk(builder.buildColumn<int64_t>(
      duration_column_name, {60, 120}));

  std::shared_ptr<arrow::RecordBatch> record_batch;
  arrowAssignOrRaise(record_batch, builder.getResult());

  arrowAssertNotOk(metadata::fillGroupMetadata(&record_batch, {tag_column_name}));
  arrowAssertNotOk(metadata::setTimeUnitMetadata(&record_batch, duration_column_name, time_utils::MINUTE));

  arrow::RecordBatchVector result;
  arrowAssignOrRaise(result, handler->handle(record_batch));

  REQUIRE(result.size() == 1);
  assertMetadataIsEqual(*record_batch, *result[0]);
}

TEST_CASE("DynamicWindowHandler preserves metadata", "[DynamicWindowHandler]") {
  WindowHandler::WindowOptions options{5s, 3s, false};

  DynamicWindowHandler::DynamicWindowOptions dynamic_window_options{
      "every", "period"
  };

  std::unique_ptr<RecordBatchHandler> handler =
      std::make_unique<DynamicWindowHandler>(
          dynamic_window_options, std::make_shared<WindowHandler>(std::move(options)));

  RecordBatchBuilder builder;
  builder.reset();
  arrowAssertNotOk(builder.setRowNumber(2));

  std::string time_column_name{"time"};
  arrowAssertNotOk(builder.buildTimeColumn<std::time_t>(
      time_column_name, {0, 10}, arrow::TimeUnit::SECOND));

  std::string tag_column_name{"tag"};
  arrowAssertNotOk(builder.buildColumn<std::string>(
      tag_column_name, {"tag_value", "tag_value"}));

  arrowAssertNotOk(builder.buildColumn<int64_t>(
      dynamic_window_options.every_column_name, {3000, 3000}));

  arrowAssertNotOk(builder.buildColumn<int64_t>(
      dynamic_window_options.period_column_name, {5000, 5000}));

  std::shared_ptr<arrow::RecordBatch> record_batch;
  arrowAssignOrRaise(record_batch, builder.getResult());

  arrowAssertNotOk(metadata::fillGroupMetadata(&record_batch, {tag_column_name}));
  arrowAssertNotOk(metadata::setTimeUnitMetadata(
      &record_batch, dynamic_window_options.every_column_name, time_utils::MILLI));
  arrowAssertNotOk(metadata::setTimeUnitMetadata(
      &record_batch, dynamic_window_options.period_column_name, time_utils::MILLI));

  arrow::RecordBatchVector result;
  arrowAssignOrRaise(result, handler->handle(record_batch));

  REQUIRE(result.size() == 1);
  assertMetadataIsEqual(*record_batch, *result[0]);
}

TEST_CASE("when every option is decreased window is emitted sooner", "[DynamicWindowHandler]") {
  WindowHandler::WindowOptions options{7s, 5s, false};

  DynamicWindowHandler::DynamicWindowOptions dynamic_window_options{
      "every", "period"
  };

  std::unique_ptr<RecordBatchHandler> handler =
      std::make_unique<DynamicWindowHandler>(
          dynamic_window_options, std::make_shared<WindowHandler>(std::move(options)));

  RecordBatchBuilder builder;
  builder.reset();
  arrowAssertNotOk(builder.setRowNumber(3));

  std::string time_column_name{"time"};
  arrowAssertNotOk(builder.buildTimeColumn<std::time_t>(
      time_column_name, {0, 3, 5}, arrow::TimeUnit::SECOND));

  arrowAssertNotOk(builder.buildColumn<int64_t>(
      dynamic_window_options.every_column_name, {5, 3, 3}));

  arrowAssertNotOk(builder.buildColumn<int64_t>(
      dynamic_window_options.period_column_name, {7, 7, 7}));

  std::shared_ptr<arrow::RecordBatch> record_batch;
  arrowAssignOrRaise(record_batch, builder.getResult());

  arrowAssertNotOk(metadata::setTimeUnitMetadata(
      &record_batch, dynamic_window_options.every_column_name, time_utils::SECOND));
  arrowAssertNotOk(metadata::setTimeUnitMetadata(
      &record_batch, dynamic_window_options.period_column_name, time_utils::SECOND));

  arrow::RecordBatchVector result;
  arrowAssignOrRaise(result, handler->handle(record_batch));

  REQUIRE(result.size() == 1);
  checkSize(result[0], 1, 3);
  checkColumnsArePresent(result[0], {time_column_name});

  checkValue<int64_t, arrow::Int64Scalar>(
      0, result[0], time_column_name, 0);
}

TEST_CASE("when every option is increased window is emitted later", "[DynamicWindowHandler]") {
  WindowHandler::WindowOptions options{7s, 5s, false};

  DynamicWindowHandler::DynamicWindowOptions dynamic_window_options{
      "every", "period"
  };

  std::unique_ptr<RecordBatchHandler> handler =
      std::make_unique<DynamicWindowHandler>(
          dynamic_window_options, std::make_shared<WindowHandler>(std::move(options)));

  RecordBatchBuilder builder;
  builder.reset();
  arrowAssertNotOk(builder.setRowNumber(2));

  std::string time_column_name{"time"};
  arrowAssertNotOk(builder.buildTimeColumn<std::time_t>(
      time_column_name, {0, 5}, arrow::TimeUnit::SECOND));

  arrowAssertNotOk(builder.buildColumn<int64_t>(
      dynamic_window_options.every_column_name, {5, 7}));

  arrowAssertNotOk(builder.buildColumn<int64_t>(
      dynamic_window_options.period_column_name, {7, 7}));

  std::shared_ptr<arrow::RecordBatch> record_batch;
  arrowAssignOrRaise(record_batch, builder.getResult());

  arrowAssertNotOk(metadata::setTimeUnitMetadata(
      &record_batch, dynamic_window_options.every_column_name, time_utils::SECOND));
  arrowAssertNotOk(metadata::setTimeUnitMetadata(
      &record_batch, dynamic_window_options.period_column_name, time_utils::SECOND));

  arrow::RecordBatchVector result;
  arrowAssignOrRaise(result, handler->handle(record_batch));

  REQUIRE(result.size() == 0);
}

TEST_CASE("when period option is decreased emitted window is shorter", "[DynamicWindowHandler]") {
  WindowHandler::WindowOptions options{7s, 5s, true};

  DynamicWindowHandler::DynamicWindowOptions dynamic_window_options{
      "every", "period"
  };

  std::unique_ptr<RecordBatchHandler> handler =
      std::make_unique<DynamicWindowHandler>(
          dynamic_window_options, std::make_shared<WindowHandler>(std::move(options)));

  RecordBatchBuilder builder;
  builder.reset();
  arrowAssertNotOk(builder.setRowNumber(4));

  std::string time_column_name{"time"};
  arrowAssertNotOk(builder.buildTimeColumn<std::time_t>(
      time_column_name, {0, 1, 6, 7}, arrow::TimeUnit::SECOND));

  arrowAssertNotOk(builder.buildColumn<int64_t>(
      dynamic_window_options.every_column_name, {5, 5, 5, 5}));

  arrowAssertNotOk(builder.buildColumn<int64_t>(
      dynamic_window_options.period_column_name, {7, 7, 6, 6}));

  std::shared_ptr<arrow::RecordBatch> record_batch;
  arrowAssignOrRaise(record_batch, builder.getResult());

  arrowAssertNotOk(metadata::setTimeUnitMetadata(
      &record_batch, dynamic_window_options.every_column_name, time_utils::SECOND));
  arrowAssertNotOk(metadata::setTimeUnitMetadata(
      &record_batch, dynamic_window_options.period_column_name, time_utils::SECOND));

  arrow::RecordBatchVector result;
  arrowAssignOrRaise(result, handler->handle(record_batch));

  REQUIRE(result.size() == 1);
  checkSize(result[0], 2, 3);
  checkColumnsArePresent(result[0], {time_column_name});

  checkValue<int64_t, arrow::Int64Scalar>(
      0, result[0], time_column_name, 0);
  checkValue<int64_t, arrow::Int64Scalar>(
      1, result[0], time_column_name, 1);
}
