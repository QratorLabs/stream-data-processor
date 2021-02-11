#include <memory>

#include <catch2/catch.hpp>

#include "record_batch_builder.h"
#include "test_help.h"

using namespace stream_data_processor;

TEST_CASE( "setting column type", "[metadata]" ) {
  std::string column_name{"field_name"};
  RecordBatchBuilder builder;
  builder.reset();
  arrowAssertNotOk(builder.setRowNumber(1));
  arrowAssertNotOk(builder.buildColumn<int64_t>(column_name, {0}, metadata::UNKNOWN));
  std::shared_ptr<arrow::RecordBatch> record_batch;
  arrowAssignOrRaise(record_batch, builder.getResult());

  REQUIRE( metadata::getColumnType(*record_batch->schema()->GetFieldByName(column_name)) != metadata::FIELD );

  arrowAssertNotOk(metadata::setColumnTypeMetadata(
      &record_batch, column_name, metadata::FIELD));

  REQUIRE( metadata::getColumnType(*record_batch->schema()->GetFieldByName(column_name)) == metadata::FIELD );
}
