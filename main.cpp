#include <fstream>

#include <gandiva/tree_expr_builder.h>

#include <spdlog/spdlog.h>

#include <uvw.hpp>

#include "data_handlers/csv_to_record_batches_converter.h"
#include "data_handlers/map_handler.h"

#include "nodes/eval_node.h"
#include "nodes/finalize_node.h"

int main() {
  auto loop = uvw::Loop::getDefault();

  std::ofstream oss("result.txt");
  FinalizeNode finalize_node("finalize_node", loop, {"127.0.0.1", 4250}, oss);

  auto field0 = arrow::field("operand1", arrow::int64());
  auto field1 = arrow::field("operand2", arrow::int64());
  auto schema = arrow::schema({field0, field1});

  auto field_sum = field("add", arrow::int64());
  auto field_subtract = field("subtract", arrow::int64());

  auto sum_expr = gandiva::TreeExprBuilder::MakeExpression("add", {field0, field1}, field_sum);
  auto subtract_expr = gandiva::TreeExprBuilder::MakeExpression("subtract", {field0, field1}, field_subtract);

  gandiva::ExpressionVector expressions{sum_expr, subtract_expr};

  EvalNode eval_node("eval_node", loop,
      std::make_shared<MapHandler>(schema, expressions),
      {"127.0.0.1", 4241},
      {
    {"127.0.0.1", 4250}
      });

  EvalNode pass_node("pass_node", loop,
      std::make_shared<CSVToRecordBatchesConverter>(),
          {"127.0.0.1", 4240},
          {
    {"127.0.0.1", 4241}
          });

  loop->run();

  oss.close();

  return 0;
}