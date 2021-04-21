#include <ctime>
#include <memory>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>

#include <catch2/catch.hpp>
#include <gmock/gmock.h>
#include <spdlog/spdlog.h>
#include <uvw.hpp>

#include "kapacitor_udf/udf_agent.h"
#include "kapacitor_udf/request_handlers/aggregate_request_handlers/aggregate_options_parser.h"
#include "kapacitor_udf/request_handlers/batch_request_handler.h"
#include "kapacitor_udf/request_handlers/invalid_option_exception.h"
#include "record_batch_handlers/aggregate_handler.h"
#include "record_batch_handlers/pipeline_handler.h"
#include "record_batch_handlers/record_batch_handler.h"
#include "kapacitor_udf/kapacitor_udf.h"
#include "kapacitor_udf/utils/grouping_utils.h"
#include "kapacitor_udf/utils/points_converter.h"
#include "kapacitor_udf/utils/points_storage.h"
#include "kapacitor_udf/request_handlers/dynamic_window_request_handler.h"
#include "test_help.h"
#include "record_batch_builder.h"

#include "udf.pb.h"

using namespace stream_data_processor;
using namespace kapacitor_udf;

class MockUDFAgent : public IUDFAgent {
 public:
  MOCK_METHOD(void, start, (), (override));
  MOCK_METHOD(void, stop, (), (override));
  MOCK_METHOD(void,
              writeResponse,
              (const agent::Response& response),
              (const, override));
};

SCENARIO("UDFAgent with RequestHandler interaction",
         "[BatchRequestHandler]") {
  GIVEN("UDFAgent and mirror Handler") {
    std::shared_ptr<MockUDFAgent>
        mock_agent = std::make_shared<MockUDFAgent>();
    std::vector<std::shared_ptr<RecordBatchHandler>> empty_handler_pipeline;
    BasePointsConverter::PointsToRecordBatchesConversionOptions
        to_record_batches_options{
        "time",
        "measurement"
    };

    std::unique_ptr<storage_utils::IPointsStorage> points_storage =
        std::make_unique<storage_utils::PointsStorage>(
            mock_agent.get(),
            std::make_unique<BasePointsConverter>(to_record_batches_options),
            std::make_unique<PipelineHandler>(std::move(empty_handler_pipeline)),
            false);

    std::shared_ptr<RequestHandler> handler =
        std::make_shared<BatchRequestHandler>(
            mock_agent.get(),
            std::move(points_storage));

    agent::Point point;
    int64_t now = std::time(nullptr) * 1000000000;
    point.set_time(now);
    point.set_name("name");
    point.set_group("");

    agent::BeginBatch begin_batch;
    begin_batch.set_group("");
    begin_batch.set_name("name");
    begin_batch.set_byname(false);

    agent::EndBatch end_batch;
    end_batch.set_group("");
    end_batch.set_name("name");
    end_batch.set_byname(false);
    end_batch.set_tmax(now);

    WHEN("mirror Handler consumes points batch") {
      THEN("UDFAgent's writeResponse method is called with same points") {
        auto serialized_point = point.SerializeAsString();
        EXPECT_CALL(*mock_agent,
                    writeResponse(::testing::Truly([serialized_point](const agent::Response& response) {
                      return response.point().SerializeAsString()
                          == serialized_point;
                    }))).Times(::testing::Exactly(1));
        EXPECT_CALL(*mock_agent,
                    writeResponse(::testing::Truly([serialized_point](const agent::Response& response) {
                      return response.point().SerializeAsString()
                          != serialized_point;
                    }))).Times(0);
      }

      handler->beginBatch(begin_batch);
      handler->point(point);
      handler->endBatch(end_batch);
    }

    WHEN("mirror Handler restores from snapshot") {
      THEN("mirror Handler sends same data") {
        auto serialized_point = point.SerializeAsString();
        EXPECT_CALL(*mock_agent,
                    writeResponse(::testing::Truly([serialized_point](const agent::Response& response) {
                      return response.point().SerializeAsString()
                          == serialized_point;
                    }))).Times(::testing::Exactly(2));
        EXPECT_CALL(*mock_agent,
                    writeResponse(::testing::Truly([serialized_point](const agent::Response& response) {
                      return response.point().SerializeAsString()
                          != serialized_point;
                    }))).Times(::testing::Exactly(0));
      }

      handler->beginBatch(begin_batch);
      handler->point(point);
      auto snapshot_response = handler->snapshot();
      handler->endBatch(end_batch);

      auto restore_request = agent::RestoreRequest();
      restore_request.set_snapshot(snapshot_response.snapshot().snapshot());
      auto restore_response = handler->restore(restore_request);
      REQUIRE(restore_response.has_restore());
      REQUIRE(restore_response.restore().success());
      handler->endBatch(end_batch);
    }
  }
}

SCENARIO("AggregateOptionsParser behavior", "[AggregateOptionsParser]") {
  GIVEN("init_request from kapacitor") {
    agent::InitRequest init_request;
    WHEN ("init_request is structured properly") {
      std::string aggregates_value{"last(field) as field.last"};
      std::string time_rule_value{"last"};

      auto aggregates_option = init_request.mutable_options()->Add();
      aggregates_option->set_name(AggregateOptionsParser::AGGREGATES_OPTION_NAME);
      auto aggregates_option_value =
          aggregates_option->mutable_values()->Add();
      aggregates_option_value->set_stringvalue(aggregates_value);

      auto time_rule_option = init_request.mutable_options()->Add();
      time_rule_option->set_name(AggregateOptionsParser::TIME_AGGREGATE_RULE_OPTION_NAME);
      auto time_rule_option_value = time_rule_option->mutable_values()->Add();
      time_rule_option_value->set_stringvalue(time_rule_value);

      THEN("parsing is successful with right options") {
        auto aggregate_options =
            AggregateOptionsParser::parseOptions(init_request.options());

        REQUIRE(aggregate_options.aggregate_columns.size() == 1);
        REQUIRE(aggregate_options.aggregate_columns.find("field")
                    != aggregate_options.aggregate_columns.end());
        REQUIRE(aggregate_options.aggregate_columns["field"].size() == 1);
        REQUIRE(
            aggregate_options.aggregate_columns["field"][0].aggregate_function
                == AggregateHandler::kLast);
        REQUIRE(
            aggregate_options.aggregate_columns["field"][0].result_column_name
                == "field.last");

        REQUIRE(aggregate_options.result_time_column_rule.aggregate_function
                    == AggregateHandler::kLast);
      }
    }
  }
}

TEST_CASE("parsing group string with measurement", "[grouping_utils]") {
  std::string measurement_column_name = "name";
  std::string measurement_value = "cpu";
  std::string tag_name_0 = "cpu";
  std::string tag_value_0 = "0";
  std::string tag_name_1 = "host";
  std::string tag_value_1 = "localhost";
  std::stringstream group_string_builder;
  group_string_builder << measurement_value << '\n'
                       << tag_name_0 << '=' << tag_value_0 << ','
                       << tag_name_1 << '=' << tag_value_1;

  auto group =
      grouping_utils::parse(group_string_builder.str(), measurement_column_name);

  REQUIRE(group.group_columns_values_size() == 3);
  REQUIRE(group.group_columns_names().columns_names_size() == 3);
  for (size_t i = 0; i < group.group_columns_values_size(); ++i) {
    const auto& column_name = group.group_columns_names().columns_names(i);
    const auto& column_value = group.group_columns_values(i);

    if (column_name == measurement_column_name) {
      REQUIRE(column_value == measurement_value);
    } else if (column_name == tag_name_0) {
      REQUIRE(column_value == tag_value_0);
    } else if (column_name == tag_name_1) {
      REQUIRE(column_value == tag_value_1);
    } else {
      INFO(fmt::format("Unexpected column name: {}", column_name));
      REQUIRE(false);
    }
  }
}

TEST_CASE("group string encoding", "[grouping_utils]") {
  std::string measurement_column_name = "name";
  std::string measurement_value = "cpu";
  std::string tag_name_0 = "cpu";
  std::string tag_value_0 = "0";
  std::string tag_name_1 = "host";
  std::string tag_value_1 = "localhost";
  std::stringstream group_string_builder;
  group_string_builder << measurement_value << '\n'
                       << tag_name_0 << '=' << tag_value_0 << ','
                       << tag_name_1 << '=' << tag_value_1;

  auto group =
      grouping_utils::parse(group_string_builder.str(), measurement_column_name);

  auto encoded_string = grouping_utils::encode(
      group, measurement_column_name,
      {
        {measurement_column_name, metadata::MEASUREMENT},
        {tag_name_0, metadata::TAG},
        {tag_name_1, metadata::TAG}
      });
  auto redecoded_group =
      grouping_utils::parse(encoded_string, measurement_column_name);

  REQUIRE(redecoded_group.group_columns_values_size() == 3);
  REQUIRE(redecoded_group.group_columns_names().columns_names_size() == 3);
  for (size_t i = 0; i < redecoded_group.group_columns_values_size(); ++i) {
    const auto& column_name =
        redecoded_group.group_columns_names().columns_names(i);
    const auto& column_value = redecoded_group.group_columns_values(i);

    if (column_name == measurement_column_name) {
      REQUIRE(column_value == measurement_value);
    } else if (column_name == tag_name_0) {
      REQUIRE(column_value == tag_value_0);
    } else if (column_name == tag_name_1) {
      REQUIRE(column_value == tag_value_1);
    } else {
      INFO(fmt::format("Unexpected column name: {}", column_name));
      REQUIRE(false);
    }
  }
}

class MockPointsConverter :
    public kapacitor_udf::convert_utils::PointsConverter {
 public:
  MOCK_METHOD(arrow::Result<arrow::RecordBatchVector>,
      convertToRecordBatches, (const agent::PointBatch& points),
      (const, override));

  MOCK_METHOD(arrow::Result<agent::PointBatch>,
      convertToPoints, (const arrow::RecordBatchVector& record_batches),
      (const, override));
};

SCENARIO("Window converter decorator calls wrappee's method", "[WindowOptionsConverterDecorator]") {
  GIVEN("decorator and wrappee") {
    using namespace ::testing;

    std::shared_ptr<MockPointsConverter> wrappee_mock =
        std::make_shared<NiceMock<MockPointsConverter>>();

    kapacitor_udf::internal::WindowOptionsConverterDecorator::WindowOptions options;
    options.period_option.emplace("period", time_utils::SECOND);
    options.every_option.emplace("every", time_utils::SECOND);

    std::unique_ptr<kapacitor_udf::convert_utils::PointsConverter> decorator =
        std::make_unique<kapacitor_udf::internal::WindowOptionsConverterDecorator>(
            wrappee_mock, options);

    WHEN("decorator converts points to RecordBatches") {
      THEN("wrappee's method is called") {
        EXPECT_CALL(*wrappee_mock,
                    convertToRecordBatches(_))
                    .WillOnce(Return(arrow::RecordBatchVector{}));
      }

      arrow::RecordBatchVector result;
      arrowAssignOrRaise(
          result, decorator->convertToRecordBatches(agent::PointBatch{}));

      REQUIRE( result.empty() );
    }

    WHEN("decorator converts RecordBatches to points") {
      THEN("wrappee's method is called") {
        EXPECT_CALL(*wrappee_mock,
                    convertToPoints(_))
            .WillOnce(Return(agent::PointBatch{}));
      }

      RecordBatchBuilder builder;
      builder.reset();
      arrowAssertNotOk(builder.setRowNumber(0));
      arrowAssertNotOk(builder.buildTimeColumn<int64_t>("time", {}, arrow::TimeUnit::SECOND));

      std::shared_ptr<arrow::RecordBatch> record_batch;
      arrowAssignOrRaise(record_batch, builder.getResult());

      agent::PointBatch result;
      arrowAssignOrRaise(
          result, decorator->convertToPoints({record_batch}));

      REQUIRE( result.points().empty() );
    }
  }
}

TEST_CASE("successfully parses DynamicWindowUDF options", "[DynamicWindowUDF]") {
  using namespace std::chrono_literals;

  std::unordered_map<std::string, agent::OptionValue> options;
  std::string period_field_option_name{"periodField"};
  options[period_field_option_name] = {};
  options[period_field_option_name].set_type(agent::STRING);
  options[period_field_option_name].set_stringvalue("period");

  std::string period_time_unit_option_name{"periodTimeUnit"};
  options[period_time_unit_option_name] = {};
  options[period_time_unit_option_name].set_type(agent::STRING);

  time_utils::TimeUnit expected_period_time_unit;
  SECTION( "set period time unit as \"ns\"" ) {
    options[period_time_unit_option_name].set_stringvalue("ns");
    expected_period_time_unit = time_utils::NANO;
  }
  SECTION( "set period time unit as \"mcs\"" ) {
    options[period_time_unit_option_name].set_stringvalue("mcs");
    expected_period_time_unit = time_utils::MICRO;
  }
  SECTION( "set period time unit as \"u\"" ) {
    options[period_time_unit_option_name].set_stringvalue("u");
    expected_period_time_unit = time_utils::MICRO;
  }
  SECTION( "set period time unit as \"us\"" ) {
    options[period_time_unit_option_name].set_stringvalue("us");
    expected_period_time_unit = time_utils::MICRO;
  }
  SECTION( "set period time unit as \"ms\"" ) {
    options[period_time_unit_option_name].set_stringvalue("ms");
    expected_period_time_unit = time_utils::MILLI;
  }
  SECTION( "set period time unit as \"s\"" ) {
    options[period_time_unit_option_name].set_stringvalue("s");
    expected_period_time_unit = time_utils::SECOND;
  }
  SECTION( "set period time unit as \"m\"" ) {
    options[period_time_unit_option_name].set_stringvalue("m");
    expected_period_time_unit = time_utils::MINUTE;
  }
  SECTION( "set period time unit as \"h\"" ) {
    options[period_time_unit_option_name].set_stringvalue("h");
    expected_period_time_unit = time_utils::HOUR;
  }
  SECTION( "set period time unit as \"d\"" ) {
    options[period_time_unit_option_name].set_stringvalue("d");
    expected_period_time_unit = time_utils::DAY;
  }
  SECTION( "set period time unit as \"w\"" ) {
    options[period_time_unit_option_name].set_stringvalue("w");
    expected_period_time_unit = time_utils::WEEK;
  }

  std::string default_period_option_name{"defaultPeriod"};
  options[default_period_option_name] = {};
  options[default_period_option_name].set_type(agent::DURATION);
  options[default_period_option_name].set_durationvalue(
      std::chrono::duration_cast<std::chrono::nanoseconds>(60s).count());

  std::string static_every_option_name{"staticEvery"};
  options[static_every_option_name] = {};
  options[static_every_option_name].set_type(agent::DURATION);
  options[static_every_option_name].set_durationvalue(
      std::chrono::duration_cast<std::chrono::nanoseconds>(30s).count());

  std::string fill_period_option_name{"fillPeriod"};

  std::string emit_timeout_option_name{"emitTimeout"};
  options[emit_timeout_option_name] = {};
  options[emit_timeout_option_name].set_type(agent::DURATION);
  options[emit_timeout_option_name].set_durationvalue(
      std::chrono::duration_cast<std::chrono::nanoseconds>(10s).count());

  google::protobuf::RepeatedPtrField<agent::Option> request_options;
  for (auto& [option_name, option_value] : options) {
    auto new_option = request_options.Add();
    new_option->set_name(option_name);
    auto new_option_value = new_option->mutable_values()->Add();
    *new_option_value = option_value;
  }

  auto fill_period_option = request_options.Add();
  fill_period_option->set_name(fill_period_option_name);

  auto parsed_options = kapacitor_udf::internal::parseWindowOptions(request_options);

  REQUIRE( (60s).count() == parsed_options.window_handler_options.period.count()  );
  REQUIRE( (30s).count() == parsed_options.window_handler_options.every.count() );
  REQUIRE( parsed_options.window_handler_options.fill_period );

  REQUIRE( parsed_options.convert_options.period_option.has_value() );
  REQUIRE( options[period_field_option_name].stringvalue() ==
              parsed_options.convert_options.period_option.value().first );
  REQUIRE( expected_period_time_unit == parsed_options.convert_options.period_option.value().second );

  REQUIRE( !parsed_options.convert_options.every_option.has_value() );
}

TEST_CASE("successfully parses DynamicWindowUDF options with alternative configuration", "[DynamicWindowUDF]") {
  using namespace std::chrono_literals;

  std::unordered_map<std::string, agent::OptionValue> options;
  std::string every_period_option_name{"staticPeriod"};
  options[every_period_option_name] = {};
  options[every_period_option_name].set_type(agent::DURATION);
  options[every_period_option_name].set_durationvalue(
      std::chrono::duration_cast<std::chrono::nanoseconds>(60s).count());

  std::string every_field_option_name{"everyField"};
  options[every_field_option_name] = {};
  options[every_field_option_name].set_type(agent::STRING);
  options[every_field_option_name].set_stringvalue("every");

  std::string every_time_unit_option_name{"everyTimeUnit"};
  options[every_time_unit_option_name] = {};
  options[every_time_unit_option_name].set_type(agent::STRING);
  options[every_time_unit_option_name].set_stringvalue("ms");

  time_utils::TimeUnit expected_every_time_unit;
  SECTION( "set every time unit as \"ns\"" ) {
    options[every_time_unit_option_name].set_stringvalue("ns");
    expected_every_time_unit = time_utils::NANO;
  }
  SECTION( "set every time unit as \"mcs\"" ) {
    options[every_time_unit_option_name].set_stringvalue("mcs");
    expected_every_time_unit = time_utils::MICRO;
  }
  SECTION( "set every time unit as \"u\"" ) {
    options[every_time_unit_option_name].set_stringvalue("u");
    expected_every_time_unit = time_utils::MICRO;
  }
  SECTION( "set every time unit as \"us\"" ) {
    options[every_time_unit_option_name].set_stringvalue("us");
    expected_every_time_unit = time_utils::MICRO;
  }
  SECTION( "set every time unit as \"ms\"" ) {
    options[every_time_unit_option_name].set_stringvalue("ms");
    expected_every_time_unit = time_utils::MILLI;
  }
  SECTION( "set every time unit as \"s\"" ) {
    options[every_time_unit_option_name].set_stringvalue("s");
    expected_every_time_unit = time_utils::SECOND;
  }
  SECTION( "set every time unit as \"m\"" ) {
    options[every_time_unit_option_name].set_stringvalue("m");
    expected_every_time_unit = time_utils::MINUTE;
  }
  SECTION( "set every time unit as \"h\"" ) {
    options[every_time_unit_option_name].set_stringvalue("h");
    expected_every_time_unit = time_utils::HOUR;
  }
  SECTION( "set every time unit as \"d\"" ) {
    options[every_time_unit_option_name].set_stringvalue("d");
    expected_every_time_unit = time_utils::DAY;
  }
  SECTION( "set every time unit as \"w\"" ) {
    options[every_time_unit_option_name].set_stringvalue("w");
    expected_every_time_unit = time_utils::WEEK;
  }

  std::string default_every_option_name{"defaultEvery"};
  options[default_every_option_name] = {};
  options[default_every_option_name].set_type(agent::DURATION);
  options[default_every_option_name].set_durationvalue(
      std::chrono::duration_cast<std::chrono::nanoseconds>(1s).count());

  std::string emit_timeout_option_name{"emitTimeout"};
  options[emit_timeout_option_name] = {};
  options[emit_timeout_option_name].set_type(agent::DURATION);
  options[emit_timeout_option_name].set_durationvalue(
      std::chrono::duration_cast<std::chrono::nanoseconds>(10s).count());

  google::protobuf::RepeatedPtrField<agent::Option> request_options;
  for (auto& [option_name, option_value] : options) {
    auto new_option = request_options.Add();
    new_option->set_name(option_name);
    auto new_option_value = new_option->mutable_values()->Add();
    *new_option_value = option_value;
  }

  auto parsed_options = kapacitor_udf::internal::parseWindowOptions(request_options);

  REQUIRE( (60s).count() == parsed_options.window_handler_options.period.count()  );
  REQUIRE( (1s).count() == parsed_options.window_handler_options.every.count() );
  REQUIRE( !parsed_options.window_handler_options.fill_period );
  REQUIRE( !parsed_options.convert_options.period_option.has_value() );

  REQUIRE( parsed_options.convert_options.every_option.has_value() );
  REQUIRE( options[every_field_option_name].stringvalue() ==
      parsed_options.convert_options.every_option.value().first );
  REQUIRE( expected_every_time_unit == parsed_options.convert_options.every_option.value().second );
}

using Catch::Matchers::Contains;
using Catch::Matchers::Equals;

TEST_CASE("static and dynamic configurations conflict", "[DynamicWindowUDF]") {
  using namespace std::chrono_literals;

  std::unordered_map<std::string, agent::OptionValue> options;
  std::string every_period_option_name{"staticPeriod"};
  options[every_period_option_name] = {};
  options[every_period_option_name].set_type(agent::DURATION);
  options[every_period_option_name].set_durationvalue(
      std::chrono::duration_cast<std::chrono::nanoseconds>(60s).count());

  std::string static_every_option_name{"staticEvery"};
  options[static_every_option_name] = {};
  options[static_every_option_name].set_type(agent::DURATION);
  options[static_every_option_name].set_durationvalue(
      std::chrono::duration_cast<std::chrono::nanoseconds>(30s).count());

  std::string every_field_option_name{"everyField"};
  options[every_field_option_name] = {};
  options[every_field_option_name].set_type(agent::STRING);
  options[every_field_option_name].set_stringvalue("every");

  std::string every_time_unit_option_name{"everyTimeUnit"};
  options[every_time_unit_option_name] = {};
  options[every_time_unit_option_name].set_type(agent::STRING);
  options[every_time_unit_option_name].set_stringvalue("ms");

  std::string default_every_option_name{"defaultEvery"};
  options[default_every_option_name] = {};
  options[default_every_option_name].set_type(agent::DURATION);
  options[default_every_option_name].set_durationvalue(
      std::chrono::duration_cast<std::chrono::nanoseconds>(30s).count());

  std::string emit_timeout_option_name{"emitTimeout"};
  options[emit_timeout_option_name] = {};
  options[emit_timeout_option_name].set_type(agent::DURATION);
  options[emit_timeout_option_name].set_durationvalue(
      std::chrono::duration_cast<std::chrono::nanoseconds>(10s).count());

  google::protobuf::RepeatedPtrField<agent::Option> request_options;
  for (auto& [option_name, option_value] : options) {
    auto new_option = request_options.Add();
    new_option->set_name(option_name);
    auto new_option_value = new_option->mutable_values()->Add();
    *new_option_value = option_value;
  }

  REQUIRE_THROWS_AS(
      kapacitor_udf::internal::parseWindowOptions(request_options),
      InvalidOptionException);

  REQUIRE_THROWS_WITH(
      kapacitor_udf::internal::parseWindowOptions(request_options),
      Contains("exactly one") && (
          Contains("every") || Contains("Every")));
}

TEST_CASE("when property configuration is missing exception is thrown", "[DynamicWindowUDF]") {
  using namespace std::chrono_literals;

  std::unordered_map<std::string, agent::OptionValue> options;
  std::string every_period_option_name{"staticPeriod"};
  options[every_period_option_name] = {};
  options[every_period_option_name].set_type(agent::DURATION);
  options[every_period_option_name].set_durationvalue(
      std::chrono::duration_cast<std::chrono::nanoseconds>(60s).count());

  std::string emit_timeout_option_name{"emitTimeout"};
  options[emit_timeout_option_name] = {};
  options[emit_timeout_option_name].set_type(agent::DURATION);
  options[emit_timeout_option_name].set_durationvalue(
      std::chrono::duration_cast<std::chrono::nanoseconds>(10s).count());

  google::protobuf::RepeatedPtrField<agent::Option> request_options;
  for (auto& [option_name, option_value] : options) {
    auto new_option = request_options.Add();
    new_option->set_name(option_name);
    auto new_option_value = new_option->mutable_values()->Add();
    *new_option_value = option_value;
  }

  REQUIRE_THROWS_AS(
      kapacitor_udf::internal::parseWindowOptions(request_options),
      InvalidOptionException);

  REQUIRE_THROWS_WITH(
      kapacitor_udf::internal::parseWindowOptions(request_options),
      Contains("exactly one") && (
          Contains("every") || Contains("Every")));
}

TEST_CASE("when unexpected time unit is set exception is thrown", "[DynamicWindowUDF]") {
  using namespace std::chrono_literals;

  std::unordered_map<std::string, agent::OptionValue> options;
  std::string every_period_option_name{"staticPeriod"};
  options[every_period_option_name] = {};
  options[every_period_option_name].set_type(agent::DURATION);
  options[every_period_option_name].set_durationvalue(
      std::chrono::duration_cast<std::chrono::nanoseconds>(60s).count());

  std::string every_field_option_name{"everyField"};
  options[every_field_option_name] = {};
  options[every_field_option_name].set_type(agent::STRING);
  options[every_field_option_name].set_stringvalue("every");

  std::string every_time_unit_option_name{"everyTimeUnit"};
  options[every_time_unit_option_name] = {};
  options[every_time_unit_option_name].set_type(agent::STRING);
  options[every_time_unit_option_name].set_stringvalue("y");

  std::string default_every_option_name{"defaultEvery"};
  options[default_every_option_name] = {};
  options[default_every_option_name].set_type(agent::DURATION);
  options[default_every_option_name].set_durationvalue(
      std::chrono::duration_cast<std::chrono::nanoseconds>(30s).count());

  std::string emit_timeout_option_name{"emitTimeout"};
  options[emit_timeout_option_name] = {};
  options[emit_timeout_option_name].set_type(agent::DURATION);
  options[emit_timeout_option_name].set_durationvalue(
      std::chrono::duration_cast<std::chrono::nanoseconds>(10s).count());

  google::protobuf::RepeatedPtrField<agent::Option> request_options;
  for (auto& [option_name, option_value] : options) {
    auto new_option = request_options.Add();
    new_option->set_name(option_name);
    auto new_option_value = new_option->mutable_values()->Add();
    *new_option_value = option_value;
  }

  REQUIRE_THROWS_AS(
      kapacitor_udf::internal::parseWindowOptions(request_options),
      InvalidOptionException);

  REQUIRE_THROWS_WITH(
      kapacitor_udf::internal::parseWindowOptions(request_options),
      Equals("Unexpected time unit shortcut: \"y\""));
}

TEST_CASE("DynamicWindowUDF info response is right", "[DynamicWindowUDF]") {
  std::shared_ptr<IUDFAgent> udf_agent =
      std::make_shared<::testing::NiceMock<MockUDFAgent>>();

  auto loop = uvw::Loop::getDefault();
  DynamicWindowRequestHandler request_handler(udf_agent.get(), loop.get());

  auto info_response = request_handler.info();

  REQUIRE( info_response.has_info() );
  REQUIRE( agent::STREAM == info_response.info().wants() );
  REQUIRE( agent::BATCH == info_response.info().provides() );

  std::unordered_map<std::string, agent::ValueType> window_options_types{
      {"periodField", agent::STRING},
      {"periodTimeUnit", agent::STRING},
      {"everyField", agent::STRING},
      {"everyTimeUnit", agent::STRING},
      {"defaultPeriod", agent::DURATION},
      {"defaultEvery", agent::DURATION},
      {"emitTimeout", agent::DURATION},
      {"staticPeriod", agent::DURATION},
      {"staticEvery", agent::DURATION}
  };

  std::string fill_period_option_name{"fillPeriod"};

  std::unordered_set<std::string> info_options;

  for (auto& option : info_response.info().options()) {
    info_options.insert(option.first);
    if (window_options_types.find(option.first) != window_options_types.end()) {
      REQUIRE( option.second.valuetypes_size() == 1 );
      REQUIRE( window_options_types[option.first] == option.second.valuetypes(0) );
    } else {
      REQUIRE( option.first == fill_period_option_name );
      REQUIRE( option.second.valuetypes_size() == 0 );
    }
  }

  REQUIRE( info_options.find(fill_period_option_name) != info_options.end() );
  for ([[maybe_unused]] auto& [option_name, _] : window_options_types) {
    REQUIRE( info_options.find(option_name) != info_options.end() );
  }
}

TEST_CASE( "DerivativeUDF info response is right", "[DerivativeUDF]" ) {
  std::unique_ptr<IUDFAgent> udf_agent =
      std::make_unique<::testing::StrictMock<MockUDFAgent>>();

  auto loop = uvw::Loop::getDefault();
  DerivativeRequestHandler request_handler(udf_agent.get(), loop.get());

  auto info_response = request_handler.info();

  REQUIRE( info_response.has_info() );
  REQUIRE( agent::STREAM == info_response.info().wants() );
  REQUIRE( agent::STREAM == info_response.info().provides() );

  std::string no_wait_future_option_name = "noWait";
  std::unordered_map<std::string, agent::ValueType> derivative_options_types{
      {"derivative", agent::STRING},
      {"as", agent::STRING},
      {"order", agent::INT},
      {"unit", agent::DURATION},
      {"neighbourhood", agent::DURATION},
      {"emitTimeout", agent::DURATION}
  };

  std::unordered_set<std::string> info_options;

  for (auto& option : info_response.info().options()) {
    info_options.insert(option.first);
    if (derivative_options_types.find(option.first) != derivative_options_types.end()) {
      REQUIRE(option.second.valuetypes_size() == 1);
      REQUIRE(derivative_options_types[option.first]
                  == option.second.valuetypes(0));
    } else {
      REQUIRE( option.first == no_wait_future_option_name );
      REQUIRE( option.second.valuetypes_size() == 0 );
    }
  }

  REQUIRE( info_options.find(no_wait_future_option_name) != info_options.end() );
  for ([[maybe_unused]] auto& [option_name, _] : derivative_options_types) {
    REQUIRE( info_options.find(option_name) != info_options.end() );
  }
}

TEST_CASE("successfully parses DerivativeUDF options", "[DerivativeUDF]") {
  using namespace std::chrono_literals;

  std::unordered_map<std::string, agent::OptionValue> options;
  google::protobuf::RepeatedPtrField<agent::Option> request_options;

  std::string derivative_option_name{"derivative"};
  options[derivative_option_name] = {};
  options[derivative_option_name].set_type(agent::STRING);
  options[derivative_option_name].set_stringvalue("cpu_idle");
  auto new_option = request_options.Add();
  new_option->set_name(derivative_option_name);
  auto new_option_value = new_option->mutable_values()->Add();
  *new_option_value = options[derivative_option_name];

  std::string result_option_name{"as"};
  options[result_option_name] = {};
  options[result_option_name].set_type(agent::STRING);
  options[result_option_name].set_stringvalue("cpu_idle_der");
  new_option = request_options.Add();
  new_option->set_name(result_option_name);
  new_option_value = new_option->mutable_values()->Add();
  *new_option_value = options[result_option_name];

  std::string order_option_name{"order"};
  options[order_option_name] = {};
  options[order_option_name].set_type(agent::INT);
  options[order_option_name].set_intvalue(2);
  new_option = request_options.Add();
  new_option->set_name(order_option_name);
  new_option_value = new_option->mutable_values()->Add();
  *new_option_value = options[order_option_name];

  std::string unit_time_segment_option_name{"unit"};
  options[unit_time_segment_option_name] = {};
  options[unit_time_segment_option_name].set_type(agent::DURATION);
  options[unit_time_segment_option_name].set_durationvalue(
      std::chrono::duration_cast<std::chrono::nanoseconds>(2s).count());
  new_option = request_options.Add();
  new_option->set_name(unit_time_segment_option_name);
  new_option_value = new_option->mutable_values()->Add();
  *new_option_value = options[unit_time_segment_option_name];

  std::string neighbourhood_option_name{"neighbourhood"};
  options[neighbourhood_option_name] = {};
  options[neighbourhood_option_name].set_type(agent::DURATION);
  options[neighbourhood_option_name].set_durationvalue(
      std::chrono::duration_cast<std::chrono::nanoseconds>(3s).count());
  new_option = request_options.Add();
  new_option->set_name(neighbourhood_option_name);
  new_option_value = new_option->mutable_values()->Add();
  *new_option_value = options[neighbourhood_option_name];

  std::string emit_timeout_option_name{"emitTimeout"};
  options[emit_timeout_option_name] = {};
  options[emit_timeout_option_name].set_type(agent::DURATION);
  options[emit_timeout_option_name].set_durationvalue(
      std::chrono::duration_cast<std::chrono::nanoseconds>(20s).count());
  new_option = request_options.Add();
  new_option->set_name(emit_timeout_option_name);
  new_option_value = new_option->mutable_values()->Add();
  *new_option_value = options[emit_timeout_option_name];

  std::string no_wait_option_name{"noWait"};
  new_option = request_options.Add();
  new_option->set_name(no_wait_option_name);

  auto parsed_options = kapacitor_udf::internal::parseDerivativeOptions(request_options);

  REQUIRE( 20s == parsed_options.emit_timeout  );
  REQUIRE( 2s == parsed_options.options.unit_time_segment );
  REQUIRE( 3s == parsed_options.options.derivative_neighbourhood );
  REQUIRE( parsed_options.options.no_wait_future );

  const auto& derivative_cases = parsed_options.options.derivative_cases;
  REQUIRE( 1 == derivative_cases.size() );
  REQUIRE( derivative_cases.find("cpu_idle_der") != derivative_cases.end() );
  REQUIRE( 2 == derivative_cases.at("cpu_idle_der").order );
  REQUIRE( "cpu_idle" == derivative_cases.at("cpu_idle_der").values_column_name );
}

TEST_CASE("successfully parses DerivativeUDF default options with multiple derivative cases", "[DerivativeUDF]") {
  using namespace std::chrono_literals;

  std::unordered_map<std::string, agent::OptionValue> options;
  google::protobuf::RepeatedPtrField<agent::Option> request_options;

  std::string derivative_option_name{"derivative"};
  options[derivative_option_name] = {};
  options[derivative_option_name].set_type(agent::STRING);
  options[derivative_option_name].set_stringvalue("cpu_idle");
  auto new_option = request_options.Add();
  new_option->set_name(derivative_option_name);
  auto new_option_value = new_option->mutable_values()->Add();
  *new_option_value = options[derivative_option_name];

  std::string result_option_name{"as"};
  options[result_option_name] = {};
  options[result_option_name].set_type(agent::STRING);
  options[result_option_name].set_stringvalue("cpu_idle_der");
  new_option = request_options.Add();
  new_option->set_name(result_option_name);
  new_option_value = new_option->mutable_values()->Add();
  *new_option_value = options[result_option_name];

  options[derivative_option_name].set_stringvalue("cpu_nice");
  new_option = request_options.Add();
  new_option->set_name(derivative_option_name);
  new_option_value = new_option->mutable_values()->Add();
  *new_option_value = options[derivative_option_name];

  options[result_option_name].set_stringvalue("cpu_nice_der");
  new_option = request_options.Add();
  new_option->set_name(result_option_name);
  new_option_value = new_option->mutable_values()->Add();
  *new_option_value = options[result_option_name];

  std::string order_option_name{"order"};
  options[order_option_name] = {};
  options[order_option_name].set_type(agent::INT);
  options[order_option_name].set_intvalue(2);
  new_option = request_options.Add();
  new_option->set_name(order_option_name);
  new_option_value = new_option->mutable_values()->Add();
  *new_option_value = options[order_option_name];

  auto parsed_options = kapacitor_udf::internal::parseDerivativeOptions(request_options);

  REQUIRE( 10s == parsed_options.emit_timeout  );
  REQUIRE( 1s == parsed_options.options.unit_time_segment );
  REQUIRE( 1s == parsed_options.options.derivative_neighbourhood );
  REQUIRE( !parsed_options.options.no_wait_future );

  const auto& derivative_cases = parsed_options.options.derivative_cases;
  REQUIRE( 2 == derivative_cases.size() );

  REQUIRE( derivative_cases.find("cpu_idle_der") != derivative_cases.end() );
  REQUIRE( 1 == derivative_cases.at("cpu_idle_der").order );
  REQUIRE( "cpu_idle" == derivative_cases.at("cpu_idle_der").values_column_name );

  REQUIRE( derivative_cases.find("cpu_nice_der") != derivative_cases.end() );
  REQUIRE( 2 == derivative_cases.at("cpu_nice_der").order );
  REQUIRE( "cpu_nice" == derivative_cases.at("cpu_nice_der").values_column_name );
}
