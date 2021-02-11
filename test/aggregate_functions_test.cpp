#include <memory>

#include <arrow/api.h>

#include <catch2/catch.hpp>

#include "record_batch_handlers/aggregate_functions/aggregate_functions.h"
#include "test_help.h"

using namespace stream_data_processor;

TEST_CASE( "mean of two even integers is integer", "[MeanAggregateFunction]" ) {
  std::shared_ptr<AggregateFunction> mean_function = std::make_shared<MeanAggregateFunction>();

  auto field = arrow::field("field_name", arrow::int64());
  auto schema = arrow::schema({field});

  arrow::Int64Builder array_builder;
  arrowAssertNotOk(array_builder.Append(0));
  arrowAssertNotOk(array_builder.Append(2));
  std::shared_ptr<arrow::Array> array;
  arrowAssertNotOk(array_builder.Finish(&array));
  auto record_batch = arrow::RecordBatch::Make(schema, 2, {array});

  std::shared_ptr<arrow::Scalar> result;
  arrowAssignOrRaise(result, mean_function->aggregate(*record_batch, "field_name"));
  arrowAssignOrRaise(result, result->CastTo(arrow::float64()));
  REQUIRE( std::static_pointer_cast<arrow::DoubleScalar>(result)->value == 1 );
}

TEST_CASE( "mean of even and odd integers is non integer", "[MeanAggregateFunction]" ) {
  std::shared_ptr<AggregateFunction> mean_function = std::make_shared<MeanAggregateFunction>();

  auto field = arrow::field("field_name", arrow::int64());
  auto schema = arrow::schema({field});

  arrow::Int64Builder array_builder;
  arrowAssertNotOk(array_builder.Append(0));
  arrowAssertNotOk(array_builder.Append(1));
  std::shared_ptr<arrow::Array> array;
  arrowAssertNotOk(array_builder.Finish(&array));
  auto record_batch = arrow::RecordBatch::Make(schema, 2, {array});

  std::shared_ptr<arrow::Scalar> result;
  arrowAssignOrRaise(result, mean_function->aggregate(*record_batch, "field_name"));
  arrowAssignOrRaise(result, result->CastTo(arrow::float64()));
  REQUIRE( std::static_pointer_cast<arrow::DoubleScalar>(result)->value == 0.5 );
}
