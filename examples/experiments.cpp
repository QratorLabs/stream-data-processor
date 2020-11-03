#include <chrono>
#include <fstream>
#include <iostream>
#include <memory>
#include <regex>
#include <string>
#include <vector>

#include <gandiva/tree_expr_builder.h>

#include <spdlog/spdlog.h>

#include <uvw.hpp>

#include <zmq.hpp>

#include "consumers/consumers.h"
#include "kapacitor_udf/kapacitor_udf.h"
#include "metadata.pb.h"
#include "metadata/column_typing.h"
#include "node_pipeline/node_pipeline.h"
#include "nodes/data_handlers/data_handlers.h"
#include "nodes/nodes.h"
#include "producers/producers.h"
#include "udf.pb.h"
#include "utils/parsers/graphite_parser.h"
#include "utils/utils.h"

int main(int argc, char** argv) {
  spdlog::set_level(spdlog::level::debug);

  auto field = arrow::field("field_name", arrow::int64());
  auto schema = arrow::schema({field});

  arrow::Int64Builder array_builder;
  array_builder.Append(0);
  array_builder.Append(2);
  std::shared_ptr<arrow::Array> array;
  array_builder.Finish(&array);
  auto record_batch = arrow::RecordBatch::Make(schema, 2, {array});

  field = record_batch->schema()->field(0);
  ColumnTyping::setColumnTypeMetadata(&field, TAG);

  spdlog::debug(record_batch->schema()
                    ->field(0)
                    ->metadata()
                    ->Get("column_type")
                    .ValueOrDie());

  return 0;
}
