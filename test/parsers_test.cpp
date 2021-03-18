#include <ctime>
#include <memory>
#include <sstream>
#include <string>

#include <arrow/api.h>

#include <catch2/catch.hpp>

#include "test_help.h"
#include "nodes/data_handlers/parsers/graphite_parser.h"

using namespace stream_data_processor;

TEST_CASE( "parse sample metric with custom separator", "[GraphiteParser]") {
  GraphiteParser::GraphiteParserOptions parser_options {{
    "measurement.measurement.field.field.region"
  }, "time", "_", "measurement"};
  std::shared_ptr<Parser> parser = std::make_shared<GraphiteParser>(parser_options);
  auto metric_buffer = std::make_shared<arrow::Buffer>("cpu.usage.idle.percent.eu-east 100");
  arrow::RecordBatchVector record_batch_vector;
  arrowAssignOrRaise(record_batch_vector, parser->parseRecordBatches(*metric_buffer));

  REQUIRE( record_batch_vector.size() == 1 );
  checkSize(record_batch_vector[0], 1, 4);

  checkColumnsArePresent(record_batch_vector[0], {
    "measurement",
    "region",
    "idle_percent",
    "time"
  });

  checkValue<std::string, arrow::StringScalar>("cpu_usage", record_batch_vector[0],
                                               "measurement", 0);
  checkValue<std::string, arrow::StringScalar>("eu-east", record_batch_vector[0],
                                               "region", 0);
  checkValue<int64_t, arrow::Int64Scalar>(100, record_batch_vector[0],
                                          "idle_percent", 0);
}

TEST_CASE( "parse using template with multiple ending", "[GraphiteParser]" ) {
  GraphiteParser::GraphiteParserOptions parser_options {{
    "region.measurement*"
  }, "time", ".", "measurement"};
  std::shared_ptr<Parser> parser = std::make_shared<GraphiteParser>(parser_options);
  auto metric_buffer = std::make_shared<arrow::Buffer>("us.cpu.load 100");
  arrow::RecordBatchVector record_batch_vector;
  arrowAssignOrRaise(record_batch_vector, parser->parseRecordBatches(*metric_buffer));

  REQUIRE( record_batch_vector.size() == 1 );
  checkSize(record_batch_vector[0], 1, 4);

  checkColumnsArePresent(record_batch_vector[0], {
      "measurement",
      "region",
      "value",
      "time"
  });

  checkValue<std::string, arrow::StringScalar>("cpu.load", record_batch_vector[0],
                                               "measurement", 0);
  checkValue<std::string, arrow::StringScalar>("us", record_batch_vector[0],
                                               "region", 0);
  checkValue<int64_t, arrow::Int64Scalar>(100, record_batch_vector[0],
                                          "value", 0);
}

TEST_CASE( "parse using multiple templates", "[GraphiteParser]" ) {
  GraphiteParser::GraphiteParserOptions parser_options {{
      "*.*.* region.region.measurement",
      "*.*.*.* region.region.host.measurement",
  }, "time", ".", "measurement"};
  std::shared_ptr<Parser> parser = std::make_shared<GraphiteParser>(parser_options);
  auto metric_buffer = std::make_shared<arrow::Buffer>("cn.south.mem-usage 50\n"
                                                       "us.west.localhost.cpu 100");
  arrow::RecordBatchVector record_batch_vector;
  arrowAssignOrRaise(record_batch_vector, parser->parseRecordBatches(*metric_buffer));

  REQUIRE( record_batch_vector.size() == 1 );
  checkSize(record_batch_vector[0], 2, 5);

  checkColumnsArePresent(record_batch_vector[0], {
      "measurement",
      "region",
      "host",
      "value",
      "time"
  });

  size_t metric_0 = 0;
  size_t metric_1 = 1;
  if (std::static_pointer_cast<arrow::StringScalar>(
      record_batch_vector[0]->GetColumnByName("measurement")->GetScalar(metric_0).ValueOrDie()
      )->value->ToString() != "mem") {
    std::swap(metric_0, metric_1);
  }

  checkValue<std::string, arrow::StringScalar>("mem-usage", record_batch_vector[0],
                                               "measurement", metric_0);
  checkValue<std::string, arrow::StringScalar>("cn.south", record_batch_vector[0],
                                               "region", metric_0);
  checkIsNull(record_batch_vector[0], "host", metric_0);
  checkValue<int64_t, arrow::Int64Scalar>(50, record_batch_vector[0],
                                          "value", metric_0);

  checkValue<std::string, arrow::StringScalar>("cpu", record_batch_vector[0],
                                               "measurement", metric_1);
  checkValue<std::string, arrow::StringScalar>("us.west", record_batch_vector[0],
                                               "region", metric_1);
  checkValue<std::string, arrow::StringScalar>("localhost", record_batch_vector[0],
                                               "host", metric_1);
  checkValue<int64_t, arrow::Int64Scalar>(100, record_batch_vector[0],
                                          "value", metric_1);
}

TEST_CASE ( "parse with filters", "[GraphiteParser]" ) {
  GraphiteParser::GraphiteParserOptions parser_options {{
          "cpu.* measurement.measurement.region",
          "mem.* measurement.measurement.host"
  }, "time", ".", "measurement"};
  std::shared_ptr<Parser> parser = std::make_shared<GraphiteParser>(parser_options);
  auto metric_buffer = std::make_shared<arrow::Buffer>("cpu.load.eu-east 100\n"
                                                       "mem.cached.localhost 256");
  arrow::RecordBatchVector record_batch_vector;
  arrowAssignOrRaise(record_batch_vector, parser->parseRecordBatches(*metric_buffer));

  REQUIRE( record_batch_vector.size() == 1 );
  checkSize(record_batch_vector[0], 2, 5);

  checkColumnsArePresent(record_batch_vector[0], {
      "measurement",
      "region",
      "host",
      "value",
      "time"
  });

  size_t metric_0 = 0;
  size_t metric_1 = 1;
  if (std::static_pointer_cast<arrow::StringScalar>(
      record_batch_vector[0]->GetColumnByName("measurement")->GetScalar(metric_0).ValueOrDie()
  )->value->ToString() != "cpu.load") {
    std::swap(metric_0, metric_1);
  }

  checkValue<std::string, arrow::StringScalar>("cpu.load", record_batch_vector[0],
                                               "measurement", metric_0);
  checkValue<std::string, arrow::StringScalar>("eu-east", record_batch_vector[0],
                                               "region", metric_0);
  checkIsNull(record_batch_vector[0], "host", metric_0);
  checkValue<int64_t, arrow::Int64Scalar>(100, record_batch_vector[0],
                                          "value", metric_0);

  checkValue<std::string, arrow::StringScalar>("mem.cached", record_batch_vector[0],
                                               "measurement", metric_1);
  checkIsNull(record_batch_vector[0], "region", metric_1);
  checkValue<std::string, arrow::StringScalar>("localhost", record_batch_vector[0],
                                               "host", metric_1);
  checkValue<int64_t, arrow::Int64Scalar>(256, record_batch_vector[0],
                                          "value", metric_1);
}

TEST_CASE( "parse and add additional tags", "[GraphiteParser]" ) {
  GraphiteParser::GraphiteParserOptions parser_options {{
    "measurement.measurement.field.region datacenter=1a"
  }, "time", ".", "measurement"};
  std::shared_ptr<Parser> parser = std::make_shared<GraphiteParser>(parser_options);
  auto metric_buffer = std::make_shared<arrow::Buffer>("cpu.usage.idle.eu-east 100");
  arrow::RecordBatchVector record_batch_vector;
  arrowAssignOrRaise(record_batch_vector, parser->parseRecordBatches(*metric_buffer));

  REQUIRE( record_batch_vector.size() == 1 );
  checkSize(record_batch_vector[0], 1, 5);

  checkColumnsArePresent(record_batch_vector[0], {
      "measurement",
      "region",
      "idle",
      "datacenter",
      "time"
  });

  checkValue<std::string, arrow::StringScalar>("cpu.usage", record_batch_vector[0],
                                               "measurement", 0);
  checkValue<std::string, arrow::StringScalar>("eu-east", record_batch_vector[0],
                                               "region", 0);
  checkValue<std::string, arrow::StringScalar>("1a", record_batch_vector[0],
                                               "datacenter", 0);
  checkValue<int64_t, arrow::Int64Scalar>(100, record_batch_vector[0],
                                          "idle", 0);
}

TEST_CASE( "parse timestamp", "[GraphiteParser]" ) {
  GraphiteParser::GraphiteParserOptions parser_options {{
    "measurement.measurement.field.field.region"
  }, "time", ".", "measurement"};
  std::shared_ptr<Parser> parser = std::make_shared<GraphiteParser>(parser_options);
  auto now = std::time(nullptr);
  std::stringstream metric_string_builder;
  metric_string_builder << "cpu.usage.idle.percent.eu-east 100 " << now;
  auto metric_buffer = arrow::Buffer::FromString(metric_string_builder.str());
  arrow::RecordBatchVector record_batch_vector;
  arrowAssignOrRaise(record_batch_vector, parser->parseRecordBatches(*metric_buffer));

  REQUIRE( record_batch_vector.size() == 1 );
  checkSize(record_batch_vector[0], 1, 4);

  checkColumnsArePresent(record_batch_vector[0], {
      "measurement",
      "region",
      "idle.percent",
      "time"
  });

  checkValue<std::string, arrow::StringScalar>("cpu.usage", record_batch_vector[0],
                                               "measurement", 0);
  checkValue<std::string, arrow::StringScalar>("eu-east", record_batch_vector[0],
                                               "region", 0);
  checkValue<int64_t, arrow::Int64Scalar>(100, record_batch_vector[0],
                                          "idle.percent", 0);
  checkValue<int64_t, arrow::TimestampScalar>(now, record_batch_vector[0],
                                          "time", 0);
}

TEST_CASE( "merge two metrics with the same tag values set into the one record", "[GraphiteParser]" ) {
  GraphiteParser::GraphiteParserOptions parser_options {{
    "measurement.measurement.field.field.region"
  }, "time", ".", "measurement"};
  std::shared_ptr<Parser> parser = std::make_shared<GraphiteParser>(parser_options);
  auto now = std::time(nullptr);
  std::stringstream metric_string_builder;
  metric_string_builder << "cpu.usage.idle.percent.eu-east 100 " << now << "\n"
                        << "cpu.usage.cpu.value.eu-east 50 " << now;
  auto metric_buffer = arrow::Buffer::FromString(metric_string_builder.str());
  arrow::RecordBatchVector record_batch_vector;
  arrowAssignOrRaise(record_batch_vector, parser->parseRecordBatches(*metric_buffer));

  REQUIRE( record_batch_vector.size() == 1 );
  checkSize(record_batch_vector[0], 1, 5);

  checkColumnsArePresent(record_batch_vector[0], {
      "measurement",
      "region",
      "idle.percent",
      "cpu.value",
      "time"
  });

  checkValue<std::string, arrow::StringScalar>("cpu.usage", record_batch_vector[0],
                                               "measurement", 0);
  checkValue<std::string, arrow::StringScalar>("eu-east", record_batch_vector[0],
                                               "region", 0);
  checkValue<int64_t, arrow::Int64Scalar>(100, record_batch_vector[0],
                                          "idle.percent", 0);
  checkValue<int64_t, arrow::Int64Scalar>(50, record_batch_vector[0],
                                          "cpu.value", 0);
}
