#include <ctime>
#include <memory>
#include <sstream>
#include <vector>

#include <catch2/catch.hpp>
#include <gmock/gmock.h>
#include <spdlog/spdlog.h>

#include "kapacitor_udf/udf_agent.h"
#include "kapacitor_udf/request_handlers/aggregate_request_handlers/aggregate_options_parser.h"
#include "kapacitor_udf/request_handlers/batch_to_stream_request_handler.h"
#include "record_batch_handlers/aggregate_handler.h"
#include "record_batch_handlers/pipeline_handler.h"
#include "record_batch_handlers/record_batch_handler.h"
#include "kapacitor_udf/utils/grouping_utils.h"
#include "kapacitor_udf/utils/points_converter.h"

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
    PointsConverter::PointsToRecordBatchesConversionOptions
        to_record_batches_options{
        "time",
        "measurement"
    };
    std::shared_ptr<RequestHandler>
        handler = std::make_shared<BatchToStreamRequestHandler>(
        mock_agent,
        to_record_batches_options,
        std::make_shared<PipelineHandler>(std::move(empty_handler_pipeline))
    );

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
