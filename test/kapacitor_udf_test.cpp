#include <ctime>
#include <memory>
#include <vector>

#include <catch2/catch.hpp>
#include <gmock/gmock.h>

#include "kapacitor_udf/udf_agent.h"
#include "kapacitor_udf/request_handlers/aggregate_options_parser.h"
#include "kapacitor_udf/request_handlers/batch_to_stream_request_handler.h"
#include "record_batch_handlers/aggregate_handler.h"
#include "record_batch_handlers/pipeline_handler.h"
#include "record_batch_handlers/record_batch_handler.h"
#include "utils/data_converter.h"

#include "udf.pb.h"

class MockUDFAgent : public IUDFAgent {
 public:
  MOCK_METHOD(void, start, (), (override));
  MOCK_METHOD(void, stop, (), (override));
  MOCK_METHOD(void, writeResponse, (const agent::Response& response), (override));
};

SCENARIO( "UDFAgent with RequestHandler interaction", "[BatchToStreamRequestHandler]" ) {
  GIVEN( "UDFAgent and mirror Handler" ) {
    std::shared_ptr<MockUDFAgent> mock_agent = std::make_shared<MockUDFAgent>();
    std::vector<std::shared_ptr<RecordBatchHandler>> empty_handler_pipeline;
    DataConverter::PointsToRecordBatchesConversionOptions to_record_batches_options{
      "time",
      "measurement"
    };
    DataConverter::RecordBatchesToPointsConversionOptions to_points_options{
        "time",
        "measurement"
    };
    std::shared_ptr<RequestHandler> handler = std::make_shared<BatchToStreamRequestHandler>(
        mock_agent,
        to_record_batches_options,
        to_points_options,
        std::make_shared<PipelineHandler>(std::move(empty_handler_pipeline))
        );

    WHEN( "mirror Handler consumes points batch" ) {
      agent::Point point;
      int64_t now = std::time(nullptr);
      point.set_time(now);
      point.set_name("name");

      THEN( "UDFAgent's writeResponse method is called with same points" ) {
        auto serialized_point = point.SerializeAsString();
        EXPECT_CALL(*mock_agent,
                    writeResponse(::testing::Truly([serialized_point](const agent::Response& response) {
                      return response.point().SerializeAsString() == serialized_point;
                    }))).Times(::testing::Exactly(1));
        EXPECT_CALL(*mock_agent,
                    writeResponse(::testing::Truly([serialized_point](const agent::Response& response) {
                      return response.point().SerializeAsString() != serialized_point;
                    }))).Times(0);
      }

      handler->beginBatch(agent::BeginBatch());
      handler->point(point);
      handler->endBatch(agent::EndBatch());
    }

    WHEN( "mirror Handler restores from snapshot" ) {
      agent::Point point;
      int64_t now = std::time(nullptr);
      point.set_time(now);
      point.set_name("name");

      THEN( "mirror Handler sends same data" ) {
        auto serialized_point = point.SerializeAsString();
        EXPECT_CALL(*mock_agent,
                    writeResponse(::testing::Truly([serialized_point](const agent::Response& response) {
                      return response.point().SerializeAsString() == serialized_point;
                    }))).Times(::testing::Exactly(2));
        EXPECT_CALL(*mock_agent,
                    writeResponse(::testing::Truly([serialized_point](const agent::Response& response) {
                      return response.point().SerializeAsString() != serialized_point;
                    }))).Times(::testing::Exactly(0));
      }

      handler->beginBatch(agent::BeginBatch());
      handler->point(point);
      auto snapshot_response = handler->snapshot();
      handler->endBatch(agent::EndBatch());

      auto restore_request = agent::RestoreRequest();
      restore_request.set_snapshot(snapshot_response.snapshot().snapshot());
      auto restore_response = handler->restore(restore_request);
      REQUIRE ( restore_response.has_restore() );
      REQUIRE( restore_response.restore().success() );
      handler->endBatch(agent::EndBatch());
    }
  }
}

SCENARIO( "AggregateOptionsParser behavior", "[AggregateOptionsParser]" ) {
  GIVEN( "init_request from kapacitor" ) {
    agent::InitRequest init_request;
    WHEN ( "init_request is structured properly" ) {
      std::string aggregates_value{"last(field) as field.last"};
      std::string time_rule_value{"last"};

      auto aggregates_option = init_request.mutable_options()->Add();
      aggregates_option->set_name(AggregateOptionsParser::AGGREGATES_OPTION_NAME);
      auto aggregates_option_value = aggregates_option->mutable_values()->Add();
      aggregates_option_value->set_stringvalue(aggregates_value);

      auto time_rule_option = init_request.mutable_options()->Add();
      time_rule_option->set_name(AggregateOptionsParser::TIME_AGGREGATE_RULE_OPTION_NAME);
      auto time_rule_option_value = time_rule_option->mutable_values()->Add();
      time_rule_option_value->set_stringvalue(time_rule_value);

      THEN( "parsing is successful with right options" ) {
        auto aggregate_options = AggregateOptionsParser::parseOptions(init_request.options());

        REQUIRE( aggregate_options.aggregate_columns.size() == 1 );
        REQUIRE( aggregate_options.aggregate_columns.find("field") != aggregate_options.aggregate_columns.end() );
        REQUIRE( aggregate_options.aggregate_columns["field"].size() == 1 );
        REQUIRE( aggregate_options.aggregate_columns["field"][0].aggregate_function == AggregateHandler::kLast );
        REQUIRE( aggregate_options.aggregate_columns["field"][0].result_column_name == "field.last" );

        REQUIRE( aggregate_options.result_time_column_rule.aggregate_function == AggregateHandler::kLast );
      }
    }
  }
}
