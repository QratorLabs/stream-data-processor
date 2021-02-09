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
#include "kapacitor_udf/request_handlers/batch_to_stream_request_handler.h"
#include "kapacitor_udf/request_handlers/invalid_option_exception.h"
#include "record_batch_handlers/aggregate_handler.h"
#include "record_batch_handlers/pipeline_handler.h"
#include "record_batch_handlers/record_batch_handler.h"
#include "kapacitor_udf/utils/grouping_utils.h"
#include "kapacitor_udf/utils/points_converter.h"
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
              (override));
};

SCENARIO("UDFAgent with RequestHandler interaction",
         "[BatchToStreamRequestHandler]") {
  GIVEN("UDFAgent and mirror Handler") {
    std::shared_ptr<MockUDFAgent>
        mock_agent = std::make_shared<MockUDFAgent>();
    std::vector<std::shared_ptr<RecordBatchHandler>> empty_handler_pipeline;
    BasePointsConverter::PointsToRecordBatchesConversionOptions
        to_record_batches_options{
        "time",
        "measurement"
    };
    std::shared_ptr<RecordBatchRequestHandler>
        handler = std::make_shared<BatchToStreamRequestHandler>(
        mock_agent
    );

    handler->setHandler(
        std::make_shared<PipelineHandler>(std::move(empty_handler_pipeline)));

    handler->setPointsConverter(
        std::make_shared<BasePointsConverter>(to_record_batches_options));

    WHEN("mirror Handler consumes points batch") {
      agent::Point point;
      int64_t now = std::time(nullptr);
      point.set_time(now);
      point.set_name("name");
      point.set_group("");

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

      handler->beginBatch(agent::BeginBatch());
      handler->point(point);
      handler->endBatch(agent::EndBatch());
    }

    WHEN("mirror Handler restores from snapshot") {
      agent::Point point;
      int64_t time = 100 * 10e9;
      point.set_time(time);
      point.set_name("name");
      point.set_group("");

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

      handler->beginBatch(agent::BeginBatch());
      handler->point(point);
      auto snapshot_response = handler->snapshot();
      handler->endBatch(agent::EndBatch());

      auto restore_request = agent::RestoreRequest();
      restore_request.set_snapshot(snapshot_response.snapshot().snapshot());
      auto restore_response = handler->restore(restore_request);
      REQUIRE (restore_response.has_restore());
      REQUIRE(restore_response.restore().success());
      handler->endBatch(agent::EndBatch());
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
  options[period_time_unit_option_name].set_stringvalue("m");

  std::string every_field_option_name{"everyField"};
  options[every_field_option_name] = {};
  options[every_field_option_name].set_type(agent::STRING);
  options[every_field_option_name].set_stringvalue("every");

  std::string every_time_unit_option_name{"everyTimeUnit"};
  options[every_time_unit_option_name] = {};
  options[every_time_unit_option_name].set_type(agent::STRING);
  options[every_time_unit_option_name].set_stringvalue("ms");

  std::string fill_period_option_name{"fillPeriod"};

  std::string default_period_option_name{"defaultPeriod"};
  options[default_period_option_name] = {};
  options[default_period_option_name].set_type(agent::DURATION);
  options[default_period_option_name].set_durationvalue(
      std::chrono::duration_cast<std::chrono::nanoseconds>(60s).count());

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

  auto fill_period_option = request_options.Add();
  fill_period_option->set_name(fill_period_option_name);

  auto parsed_options = kapacitor_udf::internal::parseWindowOptions(request_options);

  REQUIRE( (60s).count() == parsed_options.window_handler_options.period.count()  );
  REQUIRE( (1s).count() == parsed_options.window_handler_options.every.count() );
  REQUIRE( parsed_options.window_handler_options.fill_period );

  REQUIRE( parsed_options.convert_options.period_option.has_value() );
  REQUIRE( options[period_field_option_name].stringvalue() ==
              parsed_options.convert_options.period_option.value().first );
  REQUIRE( time_utils::MINUTE == parsed_options.convert_options.period_option.value().second );

  REQUIRE( parsed_options.convert_options.every_option.has_value() );
  REQUIRE( options[every_field_option_name].stringvalue() ==
      parsed_options.convert_options.every_option.value().first );
  REQUIRE( time_utils::MILLI == parsed_options.convert_options.every_option.value().second );
}

TEST_CASE("successfully parses DynamicWindowUDF options with alternative configuration", "[DynamicWindowUDF]") {
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
  REQUIRE( (30s).count() == parsed_options.window_handler_options.every.count() );
  REQUIRE( !parsed_options.window_handler_options.fill_period );
  REQUIRE( !parsed_options.convert_options.period_option.has_value() );
  REQUIRE( !parsed_options.convert_options.every_option.has_value() );
}

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
}

TEST_CASE("DynamicWindowUDF info response is right", "[DynamicWindowUDF]") {
  std::shared_ptr<IUDFAgent> udf_agent =
      std::make_shared<::testing::NiceMock<MockUDFAgent>>();

  auto loop = uvw::Loop::getDefault();
  DynamicWindowRequestHandler request_handler(udf_agent, loop.get());

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
