#include <ctime>
#include <memory>
#include <vector>

#include <catch2/catch.hpp>
#include "gmock/gmock.h"

#include "kapacitor_udf/agent.h"
#include "kapacitor_udf/request_handlers/batch_to_stream_request_handler.h"
#include "record_batch_handlers/record_batch_handler.h"
#include "utils/data_converter.h"

#include "udf.pb.h"

class MockAgent : public IAgent {
 public:
  MOCK_METHOD(void, start, (), (override));
  MOCK_METHOD(void, stop, (), (override));
  MOCK_METHOD(void, writeResponse, (const agent::Response& response), (override));
};

SCENARIO( "Agent with RequestHandler interaction", "[BatchToStreamRequestHandler]" ) {
  GIVEN( "Agent and mirror Handler" ) {
    std::shared_ptr<MockAgent> mock_agent = std::make_shared<MockAgent>();
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
        empty_handler_pipeline,
        to_record_batches_options,
        to_points_options
        );

    WHEN( "mirror Handler consumes points batch" ) {
      agent::Point point;
      int64_t now = std::time(nullptr);
      point.set_time(now);
      point.set_name("name");

      THEN( "Agent's writeResponse method is called with same points" ) {
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
      handler->restore(restore_request);
      handler->endBatch(agent::EndBatch());
    }
  }
}
