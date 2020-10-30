#include <arrow/api.h>
#include <catch2/catch.hpp>

#include "test_help.h"
#include "utils/utils.h"

TEST_CASE( "serialization and deserialization preserves schema metadata", "[Serializer]" ) {
  std::string metadata_key = "metadata";
  std::vector<std::string> metadata_values{ "batch_0", "batch_1"};

  auto metadata_0 = std::make_shared<arrow::KeyValueMetadata>();
  arrowAssertNotOk(metadata_0->Set(metadata_key, metadata_values[0]));

  auto metadata_1 = std::make_shared<arrow::KeyValueMetadata>();
  arrowAssertNotOk(metadata_1->Set(metadata_key, metadata_values[1]));

  auto field = arrow::field("field_name", arrow::int64());
  auto schema_0 = arrow::schema({field}, metadata_0);
  auto schema_1 = arrow::schema({field}, metadata_1);

  arrow::Int64Builder array_builder_0;
  arrowAssertNotOk(array_builder_0.Append(0));
  std::shared_ptr<arrow::Array> array_0;
  arrowAssertNotOk(array_builder_0.Finish(&array_0));
  auto record_batch_0 = arrow::RecordBatch::Make(schema_0, 1, {array_0});

  arrow::Int64Builder array_builder_1;
  arrowAssertNotOk(array_builder_1.Append(1));
  std::shared_ptr<arrow::Array> array_1;
  arrowAssertNotOk(array_builder_1.Finish(&array_1));
  auto record_batch_1 = arrow::RecordBatch::Make(schema_1, 1, {array_1});

  arrow::RecordBatchVector record_batches{record_batch_0, record_batch_1};
  std::vector<std::shared_ptr<arrow::Buffer>> buffers;

  arrowAssertNotOk(Serializer::serializeRecordBatches(
      record_batches, &buffers));

  for (size_t i = 0; i < 2; ++i) {
    arrow::RecordBatchVector deserialized;

    arrowAssertNotOk(Serializer::deserializeRecordBatches(
        buffers[i], &deserialized));

    REQUIRE( deserialized.size() == 1 );
    REQUIRE( deserialized[0]->Equals(*record_batches[i], true) );
  }
}